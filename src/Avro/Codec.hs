{-| This modules defines how to build Avro Codecs.

Codec's represent how to encode and decode data for
your domain types, as well as the schema with which
they will be written.

For example, below we define a type alias and how it
would be represented as an Avro record

> data Person =
>     Person {
>         personName :: Text
>       , personAge :: Int32
>     } deriving (Eq, Show)
>
> personCodec :: Codec Person
> personCodec =
>     Codec.record (TypeName "person" []) $ Person
>           <$> Codec.requiredField "name" Codec.string personName
>           <*> Codec.requiredField "age" Codec.int personAge


Furthermore, one of several variants in an Avro union can represent
an Elm custom type, by using the [`union`](Avro-Codec#union) family
of functions

>  personOrPetCodec : Codec (Result Person Pet)
>  personOrPetCodec =
>      union personCodec petCodec

Records, unions, named types, and basic types can be composed to easily
represent complex models.

-}

{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections   #-}
module Avro.Codec
    ( -- * Core Types
      Codec (..)
    , invmap, emap
    , withDocumentation, withAliases, withLogicalType

    -- * Basic Builders
    , unit, bool, int, int64, float, double, string, array, dict, enum, namedType

    -- * Working with Record Types
    , StructCodec, StructBuilder
    , record

    -- * Working with Union Types
    , maybe, union, union3, union4, union5
    , requiredField, optionalField, fallbackField
    , structField
    , dimap, lmap
    ) where

import           Avro.Internal.DList (DList)
import qualified Avro.Internal.DList as DList
import           Avro.Schema (Schema (..), Field (..), SortOrder)
import qualified Avro.Schema as Schema
import           Avro.Value (Value)
import qualified Avro.Value as Value
import           Avro.Name (TypeName)
import           Control.Monad ((>=>))
import           Data.Functor.Invariant (Invariant (..))
import           Data.Profunctor (Profunctor (..))
import           Data.Int (Int32, Int64)
import           Data.Map (Map)
import           Data.Text (Text)

import qualified Prelude
import           Prelude hiding (maybe)


{-| This type defines the schema, encoder, and decoder for a Haskell type.
-}
data Codec a =
  Codec {
      schema :: Schema
    , decoder :: Value -> Maybe a
    , writer :: a -> Value
    }


instance Invariant Codec where
  invmap f g Codec {..} =
    Codec
      { schema = schema
      , decoder = fmap f . decoder
      , writer = writer . g
      }

{-| Partially map a Codec to a new type.

Like `imap` this function requires a _from_ and _to_
mapping to the new type, but this also allows one to fail to parse an
unexpected value.

For example, the Logical UUID Type is backed by an Avro string.
As not all strings are valid UUIDs, this function would be used to
print and parse the string.

This function can be overused as if the mapping fails the user
will receive a parse error from the binary decoder, and lose any
specific information as to the nature of the error.

-}
emap :: (a -> Maybe b) -> (b -> a) -> Codec a -> Codec b
emap f g Codec {..} =
  Codec
    { schema = schema
    , decoder = decoder >=> f
    , writer = writer . g
    }

{-| Definition of a Struct Codec

Usually one will work with this type alias
directly, as the builder is most useful when
contructing the final value.

-}
type StructCodec a =
  StructBuilder a a


{-| Builder for a Struct Codec.

The first type parameter `b` is the type we're mapping from when writing
an Avro record, the second type parameter `a` is the type we're reading
avro data into. When a builder is completely constructed, the types `a`
and `b` will be for the same type, and can be turned into a `Codec a`
using the `record` function.

The technical term for a type like this is that `StructBuilder` is an
applicative profunctor; it wraps a parser and a builder for lists of
Avro fields (along with their Schemas).

-}
data StructBuilder b a =
  StructBuilder
    { structSchemas :: DList Field
    , structDecoder :: [Value] -> Maybe ( [Value], a )
    , structWriter :: b -> DList Value
    }


instance Functor (StructBuilder b) where
  fmap = rmap


instance Applicative (StructBuilder b) where
  pure a =
    StructBuilder
      { structSchemas = DList.empty
      , structDecoder = \fs -> Just ( fs, a )
      , structWriter = const DList.empty
      }

  parseFunc <*> parseArg =
    let
        structSchemas' =
            DList.append
                (structSchemas parseFunc)
                (structSchemas parseArg)

        structDecoder' values = do
            (remaining, f) <- structDecoder parseFunc values
            (left, a)      <- structDecoder parseArg remaining
            return (left, f a)

        structWriter' c =
            DList.append
                (structWriter parseFunc c)
                (structWriter parseArg c)
    in
    StructBuilder {
      structSchemas = structSchemas'
    , structDecoder = structDecoder'
    , structWriter = structWriter'
    }


instance Profunctor StructBuilder where
  dimap g f StructBuilder {..} =
    StructBuilder
      { structSchemas = structSchemas
      , structDecoder = \values -> f <$$> structDecoder values
      , structWriter = structWriter . g
      }


(<$$>) :: (Functor f, Functor g) => (a -> b) -> f (g a) -> f (g b)
(<$$>) = fmap . fmap
infixl 4 <$$>


{-| Add documentation to a Codec.

If the Schema does not support documentation (i.e, it's not a Record or Enum)
this function has no effect.

-}
withDocumentation :: String -> Codec a -> Codec a
withDocumentation docs codec =
    codec { schema = Schema.withDocumentation docs (schema codec) }


{-| Add aliases to a Codec.

If the Schema does not support aliases (i.e, it's not a named type)
this function has no effect.

-}
withAliases :: [TypeName] -> Codec a -> Codec a
withAliases docs codec =
    codec { schema = Schema.withAliases docs (schema codec) }


{-| Add a logical type annotation to a Codec.
-}
withLogicalType :: String -> Codec a -> Codec a
withLogicalType logicalType codec =
    codec { schema = Schema.withLogicalType logicalType (schema codec) }



{-| Compose a required field's Codecs to build a record.

The explicit arguments one should write are:

  - The field name which will be written into the Avro Schema;
  - The Codec for the individual field; and
  - How to extract the fields from the type in order to write it.

The final argument in the type signature is the pipelined builder for the
record under construction.

-}
requiredField :: Text -> Codec a -> (c -> a) -> StructBuilder c a
requiredField fieldName parseArg argExtract =
    lmap argExtract $
        structField fieldName [] Nothing Nothing parseArg Nothing

{-| Compose an optional field's Codec to build a record.

This will create a union in the Schema with null as the first field
and set a default value of null.

-}
optionalField :: Text -> Codec a -> (c -> Maybe a) -> StructBuilder c (Maybe a)
optionalField fieldName parseArg argExtract =
    let
        optCodec =
            maybe parseArg
    in
    lmap argExtract $
        structField fieldName [] Nothing Nothing optCodec (Just Nothing)


{-| Use a field in a struct codec which falls back if it doesn't exist.

In the avro specification, the default value for Union values must be
a value from the first sub-schema of the union. If the default value
violates this contstraint it will not be emitted when serializing the
Schema to JSON.

-}
fallbackField :: Text -> Codec a -> a -> (c -> a) -> StructBuilder c a
fallbackField fieldName parseArg fallback argExtract =
    lmap argExtract $
        structField fieldName [] Nothing Nothing parseArg (Just fallback)


{-| Construct a struct parser from a Codec.
-}
structField :: Text -> [Text] -> Maybe String -> Maybe SortOrder -> Codec a -> Maybe a -> StructCodec a
structField fieldName aliases docs order Codec {..} defaultValue =
    let
        structSchemas =
          DList.singleton $
            Field fieldName aliases docs order schema (writer <$> defaultValue)

        structDecoder values =
            case values of
                g : gs ->
                    (gs, ) <$>
                        decoder g

                _ ->
                    Nothing

        structWriter c =
            DList.singleton $
              writer c
    in
    StructBuilder {
      structSchemas = structSchemas
    , structDecoder = structDecoder
    , structWriter = structWriter
    }



{-| Build a Codec for an Avro record from a StructCodec.

This function requires a "completed" StructCodec, which writes and reads
the same value.

-}
record :: TypeName -> StructCodec a -> Codec a
record name StructBuilder {..} =
    let
        schema =
            Schema.Record
                name
                []
                Nothing
                (DList.toList structSchemas)

        decoder v =
            case v of
                Value.Record rs ->
                    snd <$> structDecoder rs

                _ ->
                    Nothing

        writer v =
            Value.Record (DList.toList (structWriter v))
    in
    Codec schema decoder writer




{-| Construct a Codec for an Avro union.

As Avro unions can not be nested (i.e., they can not directly contain
other unions) this function takes care to flatten unions passed in to
it.

Often it is useful to use the [`imap`](Avro-Codec#imap) function to
turn this into a Custom Type.

-}
union :: Codec a -> Codec b -> Codec (Either a b)
union left right =
    case ( schema left, schema right ) of
        ( Schema.Union lopt, Schema.Union ropt ) ->
            let
                leftSize =
                    length lopt

                uSchema =
                    Schema.Union $
                        lopt <> ropt

                uDecoder v =
                    case v of
                        Value.Union ix inner ->
                            if ix < leftSize then
                                Left <$>
                                    decoder left (Value.Union ix inner)

                            else
                                Right <$>
                                    decoder right (Value.Union (ix - leftSize) inner)

                        _ ->
                            Nothing

                uWriter v =
                    case v of
                        Left l ->
                            writer left l

                        Right r ->
                            case writer right r of
                                Value.Union ix written ->
                                    Value.Union (ix + leftSize) written

                                other ->
                                    other
            in
            Codec uSchema uDecoder uWriter

        ( Schema.Union lopt, rightSchema ) ->
            let
                leftSize =
                    length lopt

                uSchema =
                    Schema.Union $
                        (<>) lopt [ rightSchema ]


                uDecoder v =
                    case v of
                        Value.Union ix inner ->
                            if ix < leftSize then
                                Left <$>
                                    decoder left (Value.Union ix inner)

                            else
                                Right <$>
                                    decoder right inner

                        _ ->
                            Nothing

                uWriter v =
                    case v of
                        Left l ->
                            writer left l

                        Right r ->
                            Value.Union leftSize (writer right r)
            in
            Codec uSchema uDecoder uWriter

        ( leftSchema, Schema.Union ropt ) ->
            let
                uSchema =
                    Schema.Union $
                       [ leftSchema ] <> ropt

                uDecoder v =
                    case v of
                        Value.Union ix inner ->
                            if ix == 0 then
                                Left <$>
                                    decoder left inner

                            else
                                Right <$>
                                    decoder right (Value.Union (ix - 1) inner)

                        _ ->
                            Nothing

                uWriter v =
                    case v of
                        Left l ->
                            Value.Union 0 (writer left l)

                        Right r ->
                            case writer right r of
                                Value.Union ix written ->
                                    Value.Union (ix + 1) written

                                other ->
                                    other
            in
            Codec uSchema uDecoder uWriter

        ( leftSchema, rightSchema ) ->
            let
                uSchema =
                    Schema.Union
                        [ leftSchema, rightSchema ]


                uDecoder v =
                    case v of
                        Value.Union 0 inner ->
                            Left <$>
                                decoder left inner

                        Value.Union 1 inner ->
                            Right <$>
                                decoder right inner

                        _ ->
                            Nothing

                uWriter v =
                    case v of
                        Left l ->
                            Value.Union 0 (writer left l)

                        Right r ->
                            Value.Union 1 (writer right r)
            in
            Codec uSchema uDecoder uWriter




{-| A codec for a potentially missing value.

If using this in a record, it may be best to use the
`optional` function instead, as that will apply this
function as well as setting a default value.

-}
maybe :: Codec b -> Codec (Maybe b)
maybe just =
    let
        hush =
            Prelude.either (const Nothing) Just

        note =
            Prelude.maybe (Left ()) Right
    in
    invmap hush note $
        union unit just


{-| Construct a union from 3 codecs.
-}
union3 :: Codec a -> Codec b -> Codec c -> Codec (Either a (Either b c))
union3 a b c =
     a `union` union b c


{-| Construct a union from 4 codecs.
-}
union4 :: Codec a -> Codec b -> Codec c -> Codec d -> Codec (Either a (Either b (Either c d)))
union4 a b c d =
    a `union` union3 b c d


{-| Construct a union from 5 codecs.
-}
union5 :: Codec a -> Codec b -> Codec c -> Codec d -> Codec e -> Codec (Either a (Either b (Either c (Either d e))))
union5 a b c d e =
    a `union` union4 b c d e



{-| Construct a Avro enumeration encoded with a 0 base index.

This can be used with [`emap`](Avro-Codec#emap) to map to a custom type.

-}
enum :: TypeName -> [Text] -> Codec Int
enum name symbols =
    let
        schema =
            Schema.Enum
                name
                []
                Nothing
                symbols
                Nothing

        parse v =
            case v of
                Value.Enum ix ->
                    Just ix

                _ ->
                    Nothing

        render a =
            Value.Enum a
    in
    Codec schema parse render


{-| A Codec for an avro null type
-}
unit :: Codec ()
unit =
    let
        parse v =
            case v of
                Value.Null ->
                    Just ()

                _ ->
                    Nothing
    in
    Codec Schema.Null parse (const Value.Null)



{-| A Codec for a boolean type
-}
bool :: Codec Bool
bool =
    let
        parse v =
            case v of
                Value.Boolean b ->
                    Just b

                _ ->
                    Nothing
    in
    Codec Schema.Boolean parse Value.Boolean



{-| A Codec for an int type
-}
int :: Codec Int32
int =
    let
        parse v =
            case v of
                Value.Int i ->
                    Just i

                _ ->
                    Nothing
    in
    Codec (Schema.Int Nothing) parse Value.Int


{-| A Codec for a long type.
-}
int64 :: Codec Int64
int64 =
    let
        parse v =
            case v of
                Value.Long i ->
                    Just i

                _ ->
                    Nothing
    in
    Codec (Schema.Long Nothing) parse Value.Long


{-| A Codec for a float type
-}
float :: Codec Float
float =
    let
        parse v =
            case v of
                Value.Float i ->
                    Just i

                _ ->
                    Nothing
    in
    Codec Schema.Float parse Value.Float


{-| A Codec for a float type
-}
double :: Codec Double
double =
    let
        parse v =
            case v of
                Value.Double i ->
                    Just i

                _ ->
                    Nothing
    in
    Codec Schema.Double parse Value.Double


{-| A Codec for a string type
-}
string :: Codec Text
string =
    let
        parse v =
            case v of
                Value.String i ->
                    Just i

                _ ->
                    Nothing
    in
    Codec (Schema.String Nothing) parse Value.String

{-| A Codec for an array type
-}
array :: Codec a -> Codec [a]
array element =
    let
        aSchema =
            Schema.Array $
                schema element


        aDecoder v =
            case v of
                Value.Array items ->
                    traverse (decoder element) items

                _ ->
                    Nothing

        aWriter vs =
            Value.Array $
                writer element <$> vs

    in
    Codec aSchema aDecoder aWriter


{-| A Codec for an map type
-}
dict :: Codec a -> Codec (Map Text a)
dict element =
    let
        aSchema =
            Schema.Map $
                schema element


        aDecoder v =
            case v of
                Value.Map items ->
                    traverse (decoder element) items

                _ ->
                    Nothing

        aWriter vs =
            Value.Map $
                writer element <$> vs

    in
    Codec aSchema aDecoder aWriter


{-| Use a Codec as a Named Type.

The Schema for this Codec will only be its name. This can be
useful to separate definitions logically and share them.

When using this function, one will need to put the original
schema in an environment before resolving reader and writer
schemas.
-}
namedType :: Codec a -> Codec a
namedType input =
  Codec
    { schema = Schema.NamedType (Schema.typeName (schema input))
    , decoder = decoder input
    , writer = writer input
    }
