{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE CPP #-}
module Avro.Schema
    ( Schema(..)
    , Field (..)
    , SortOrder(..)
    , typeName
    , SchemaMismatch(..)

    , SchemaInvalid (..)
    , validateSchema
    )
where

import qualified Avro.Name as Name
import           Avro.Name (TypeName (..))
import qualified Avro.Value as Avro

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson

#if MIN_VERSION_aeson(2,0,0)
import qualified Data.Aeson.KeyMap as KeyMap
import           Data.Aeson.Key (Key)
import qualified Data.Aeson.Key as Key
#else
import qualified Data.HashMap.Lazy as KeyMap
#endif

import           Data.Bifunctor (bimap)
import           Data.Foldable (asum)
import qualified Data.List as List
import qualified Data.Maybe as Maybe
import qualified Data.Map as Map
import qualified Data.Scientific as Scientific
import           Data.String (IsString(..))
import qualified Data.Text as Text
import           Data.Text (Text)
import           Data.Traversable (for)
import qualified Data.Vector as Boxed


{-| Field Sort ordering
-}
data SortOrder
    = Ascending
    | Descending
    | Ignore
    deriving (Eq, Ord, Show)


{-| The Field of a Record
-}
data Field =
  Field
    { fieldName :: Text
    , fieldAliases :: [Text]
    , fieldDoc :: Maybe String
    , fieldOrder :: Maybe SortOrder
    , fieldType :: Schema
    , fieldDefault :: Maybe Avro.Value
    }
    deriving (Eq, Ord, Show)


{-| An Avro Schema
-}
data Schema
    = Null
    | Boolean
    | Int (Maybe String)
    | Long (Maybe String)
    | Float
    | Double
    | Bytes (Maybe String)
    | String (Maybe String)
    | Array Schema
    | Map Schema
    | NamedType TypeName
    | Record TypeName [TypeName] (Maybe String) [Field]
    | Enum TypeName [TypeName] (Maybe String) [Text] (Maybe Text)
    | Union [Schema]
    | Fixed TypeName [TypeName] Int (Maybe String)
    deriving (Eq, Ord, Show)

{-| Get the TypeName for an Avro Schema

For primitive types, this is an unqualified name, but for
complex types it may be qualified.

-}
typeName :: Schema -> TypeName
typeName s =
    case s of
        Null ->
            TypeName "null" []

        Boolean ->
            TypeName "boolean" []

        Int _ ->
            TypeName "int" []

        Long _ ->
            TypeName "long" []

        Float ->
            TypeName "float" []

        Double ->
            TypeName "double" []

        Bytes _ ->
            TypeName "bytes" []

        String _ ->
            TypeName "string" []

        Array _ ->
            TypeName "array" []

        Map _ ->
            TypeName "map" []

        Union _ ->
            TypeName "union" []

        NamedType name ->
            name

        Fixed name _ _ _ ->
            name

        Record name _ _ _ ->
            name

        Enum name _ _ _  _ ->
            name


data SchemaInvalid
    = SchemaNestedUnion
    | SchemaIdenticalNamesInUnion TypeName
    deriving (Eq, Show)


{-| Validates a schema to ensure:

  [ ] All types are defined
  [*] Unions do not directly contain other unions
  [*] Unions are not ambiguous (may not contain more than one schema with
      the same type except for named types of record, fixed and enum)
  [ ] Default values for unions can be cast as the type indicated by the
      first structure.
  [ ] Default values can be cast/de-serialize correctly.
  [ ] Named types are resolvable
-}
validateSchema :: Schema -> Either SchemaInvalid Schema
validateSchema =
    go True
        where
    go allowUnionsHere = \case
        Union xs | allowUnionsHere ->
            let
                sortedNames =
                    List.sort $
                        typeName <$> xs

                firstDuplicate =
                    findDuplicate sortedNames

            in
            case firstDuplicate of
                Nothing ->
                    Union <$>
                        traverse (go False) xs

                Just tn ->
                    Left (SchemaIdenticalNamesInUnion tn)

        Union _ ->
            Left SchemaNestedUnion

        Array xs ->
            Array <$> validateSchema xs

        Map xs ->
            Map <$> validateSchema xs

        Record name aliases doc fields ->
            Record name aliases doc <$>
                traverse goField fields

        primitive ->
            pure primitive

    goField fld = do
        inner <-
            validateSchema (fieldType fld)

        pure (fld { fieldType = inner })

    findDuplicate
        (a:b:rest) | a == b = Just a
                   | otherwise = findDuplicate (b:rest)

    findDuplicate
        _ = Nothing


{-| Errors which can occur when trying to read Avro with
an incompatible Schema.
-}
data SchemaMismatch
    = TypeMismatch Schema Schema
    | MissingField TypeName Text
    | FieldMismatch TypeName Text SchemaMismatch
    | MissingUnion TypeName
    | MissingSymbol Text
    | NamedTypeUnresolved TypeName
    | FixedWrongSize TypeName Int Int
    deriving (Eq, Ord, Show)




------------------------------------------------------------
--                                                        --
-- JSON codecs                                            --
--                                                        --
-- This isn't written very idiomatically for Aeson, and   --
-- is a more direct port of the Elm value level code.     --
--                                                        --
-- The main issue is that we can't decode Values without  --
-- a Schema, so it's all a bit tricky with type classes.  --
--                                                        --
------------------------------------------------------------

keyFromText :: Text -> Key

#if MIN_VERSION_aeson(2,0,0)
keyFromText = Key.fromText
#else
keyFromText = id
type Key = Text
#endif

index :: Int -> [a] -> Maybe a
index i xs =
    case List.drop i xs of
        [] ->
            Nothing
        a : _ ->
            Just a



encodeDefaultValue :: Schema -> Avro.Value -> Aeson.Value
encodeDefaultValue schema v =
    case ( schema, v ) of
        ( Union options , Avro.Union 0 ls ) ->
            case options of
                s : _ ->
                    encodeValue s ls

                _ ->
                    Aeson.Null

        ( Union _, _ ) ->
            Aeson.Null

        _ ->
            encodeValue schema v


decodeDefaultValue :: Schema -> Aeson.Value -> Aeson.Parser Avro.Value
decodeDefaultValue schema json =
    case schema of
        Union options ->
            case options of
                s : _ ->
                    Avro.Union 0 <$>
                        decodeValue s json

                _ ->
                    fail "Empty union schema, can't decode default value"

        _ ->
            decodeValue schema json


encodeValue :: Schema -> Avro.Value -> Aeson.Value
encodeValue schema v =
    case ( schema, v ) of
        ( Null, Avro.Null ) ->
            Aeson.Null

        ( Boolean, Avro.Boolean b ) ->
            Aeson.Bool b

        ( Int _, Avro.Int i ) ->
            Aeson.toJSON i

        ( Long _, Avro.Long l ) ->
            Aeson.toJSON l

        ( Float, Avro.Float l ) ->
            Aeson.toJSON l

        ( Double, Avro.Double l ) ->
            Aeson.toJSON l

        ( String _, Avro.String s ) ->
            Aeson.toJSON s

        ( Enum _ _ _ symbols _ , Avro.Enum ix ) ->
            maybe Aeson.Null Aeson.toJSON (index ix symbols)

        ( Array items, Avro.Array ls ) ->
            encodeList (encodeValue items) ls

        ( Map values, Avro.Map ls ) ->
            encodeDict keyFromText (encodeValue values) ls

        ( Union _, Avro.Union _ Avro.Null ) ->
            Aeson.Null

        ( Union options, Avro.Union ix ls ) ->
            case index ix options of
                Just s ->
                    Aeson.object
                        [ ( keyFromText (Name.baseName (typeName s)), encodeValue s ls ) ]

                Nothing ->
                    Aeson.Null

        ( Record _ _ _ fields , Avro.Record ls ) ->
            Aeson.object $
                List.zipWith
                    (\f i -> ( keyFromText (fieldName f), encodeValue (fieldType f) i ))
                    fields
                    ls


        ( Bytes _, Avro.Bytes _bytes ) ->
            -- TODO encodeBytes bytes
            Aeson.Null

        ( Fixed {}, Avro.Fixed _bytes ) ->
            -- encodeBytes bytes
            Aeson.Null

        _ ->
            Aeson.Null



decodeValue :: Schema -> Aeson.Value -> Aeson.Parser Avro.Value
decodeValue schema obj =
    case schema of
        Null ->
            case obj of
                Aeson.Null ->
                    pure Avro.Null

                _ ->
                    fail "Null value expected"

        Boolean ->
            case obj of
                Aeson.Bool b ->
                    pure (Avro.Boolean b)

                _ ->
                    fail "Boolean value expected"

        Int _ ->
            case obj of
                Aeson.Number s
                    | Just i <- Scientific.toBoundedInteger s
                    -> pure (Avro.Int i)

                _ ->
                    fail "Int value expected"

        Long _ ->
            case obj of
                Aeson.Number s
                    | Just i <- Scientific.toBoundedInteger s
                    -> pure (Avro.Long i)

                _ ->
                    fail "Long value expected"

        Float ->
            case obj of
                Aeson.Number s
                    | Right f <- Scientific.toBoundedRealFloat s
                    -> pure (Avro.Float f)

                _ ->
                    fail "Float value expected"

        Double ->
            case obj of
                Aeson.Number s
                    | Right f <- Scientific.toBoundedRealFloat s
                    -> pure (Avro.Double f)

                _ ->
                    fail "Double value expected"

        String _ ->
            case obj of
                Aeson.String s ->
                    pure (Avro.String s)

                _ ->
                    fail "String value expected"

        Array items ->
            case obj of
                Aeson.Array vs ->
                    Avro.Array . Boxed.toList <$>
                        traverse (decodeValue items) vs

                _ ->
                    fail "Boolean value expected"

        Map values ->
            Avro.Map <$>
                decodeDict
                    (decodeValue values)
                    obj

        Union options ->
            let
                choice (ix, option) =
                    case option of
                        Null ->
                            case obj of
                                Aeson.Null ->
                                    pure $
                                        Avro.Union ix Avro.Null

                                _ ->
                                    fail "Null value expected"

                        other ->
                            case obj of
                                Aeson.Object o ->  do
                                    let
                                        choiceKey =
                                            keyFromText $
                                                baseName (typeName other)
                                    value
                                        <- Aeson.explicitParseField
                                                (decodeValue other)
                                                o
                                                choiceKey

                                    return $
                                        Avro.Union ix value

                                _ ->
                                    fail "Object value expected"


                choices =
                    fmap choice (List.zip [0..] options)

            in
            asum choices


        Record _ _ _ fields ->
            case obj of
                Aeson.Object o ->
                    let
                        step acc = \case
                            [] ->
                                pure (reverse acc)

                            x : xs -> do
                                let
                                    fieldKey =
                                        keyFromText $
                                            fieldName x
                                a <- Aeson.explicitParseField
                                            (decodeValue (fieldType x))
                                            o
                                            fieldKey

                                step (a : acc) xs
                    in
                    Avro.Record <$> step [] fields

                _ ->
                    fail "Object value expected"


        Enum _ _ _ symbols _ ->
            case obj of
                Aeson.String s ->
                    case List.elemIndex s symbols of
                        Nothing ->
                            fail "Unknown enumeration value for schema"

                        Just n ->
                            pure $
                                Avro.Enum n

                _ ->
                    fail "String value expected for enumeration"


        Bytes {} ->
            fail "Can't parse bytes just yet."

        Fixed {} ->
            fail "Can't parse fixed just yet."

        NamedType {} ->
            fail "Can't parse named type value. Normalise first"


instance Aeson.FromJSON Schema where
    parseJSON =
        decodeSchema


instance Aeson.ToJSON Schema where
    toJSON =
        encodeSchema


encodeSchema :: Schema -> Aeson.Value
encodeSchema =
      encodeSchemaInContext Nothing


encodeSchemaInContext :: Maybe TypeName -> Schema -> Aeson.Value
encodeSchemaInContext context s =
    case s of
        Null ->
            Aeson.String "null"


        Boolean ->
            Aeson.String "boolean"


        Int logicalType ->
            case logicalType of
                Nothing ->
                    Aeson.String "int"

                Just lt ->
                    Aeson.object
                        [ ( "type", Aeson.String "int" )
                        , ( "logicalType", Aeson.String (fromString lt) )
                        ]

        Long logicalType ->
            case logicalType of
                Nothing ->
                    Aeson.String "long"

                Just lt ->
                    Aeson.object
                        [ ( "type", Aeson.String "long" )
                        , ( "logicalType", Aeson.String (fromString lt) )
                        ]

        Float ->
            Aeson.String "float"

        Double ->
            Aeson.String "double"

        Bytes logicalType ->
            case logicalType of
                Nothing ->
                    Aeson.String "bytes"

                Just lt ->
                    Aeson.object
                        [ ( "type", Aeson.String "bytes" )
                        , ( "logicalType", Aeson.String (fromString lt) )
                        ]

        String logicalType ->
            case logicalType of
                Nothing ->
                    Aeson.String "string"

                Just lt ->
                    Aeson.object
                        [ ( "type", Aeson.String "string" )
                        , ( "logicalType", Aeson.String (fromString lt) )
                        ]

        Array info ->
            Aeson.object
                [ ( "type", Aeson.String "array" )
                , ( "items", encodeSchemaInContext context info )
                ]

        Map info ->
            Aeson.object
                [ ( "type", Aeson.String "map" )
                , ( "values", encodeSchemaInContext context info )
                ]

        Union options ->
            encodeList
                (encodeSchemaInContext context)
                options

        NamedType nm ->
            encodeText . baseName $
                    Name.canonicalName nm

        Record name aliases doc fields ->
            let
                nameParts =
                    encodeNameParts context name aliases

                required =
                    [ ( "type", Aeson.String "record" )
                    , ( "fields", encodeList encodeField fields )
                    ]

                optionals =
                    [ ( "doc", fmap encodeString doc )
                    ]

                encodeField Field {..} =
                    let
                        nameField =
                            [ ( fromString "name", encodeText fieldName )
                            ]

                        aliasField =
                            if List.null fieldAliases then
                                []

                            else
                                [ (  fromString "aliases", encodeList encodeText fieldAliases ) ]

                        typeField =
                            [ (  fromString "type", encodeSchemaInContext (Just name) fieldType )
                            ]

                        fieldOptionals =
                            [ (  fromString "doc", fmap encodeString fieldDoc )
                            , (  fromString "order", fmap encodeSortOrder fieldOrder )
                            , (  fromString "default", fmap (encodeDefaultValue fieldType) fieldDefault)
                            ]
                    in
                    Aeson.object $
                        nameField
                            <> aliasField
                            <> typeField
                            <> encodeOptionals fieldOptionals
            in
            Aeson.object $
                nameParts
                    <> required
                    <> encodeOptionals optionals

        Fixed name aliases size logicalType ->
            let
                nameParts =
                    encodeNameParts context name aliases

                required =
                    [ ( "type", Aeson.String "fixed" )
                    , ( "size", Aeson.Number (fromIntegral size) )
                    ]

                optionals =
                    [ ( "logicalType", fmap encodeString logicalType )
                    ]

            in
            Aeson.object $
                nameParts
                    <> required
                    <> encodeOptionals optionals


        Enum name aliases doc symbols default_ ->
            let
                nameParts =
                    encodeNameParts context name aliases

                required =
                    [ ( "type", Aeson.String "enum" )
                    , ( "symbols", encodeList encodeText symbols )
                    ]

                optionals =
                    [ ( "doc", fmap encodeString doc )
                    , ( "default", fmap encodeText default_ )
                    ]

            in
            Aeson.object $
                nameParts
                    <> required
                    <> encodeOptionals optionals


encodeString :: String -> Aeson.Value
encodeString = Aeson.String . fromString


encodeText :: Text -> Aeson.Value
encodeText = Aeson.String


encodeNameParts :: Maybe TypeName -> TypeName -> [TypeName] -> [ ( Key, Aeson.Value ) ]
encodeNameParts context name aliases =
    let
        --
        -- When writing the canonical representation, you normalise the schema
        -- name to be fully qualified and don't include the namespace at all.
        --
        -- But, items without a namespace should also be written without the record
        -- in this case, but if we just blindly omit the namespace it will intead
        -- inherit it. So we add a small check to ensure if the context is the null
        -- namespace and we don't have one ourselves, we can inherit the null
        -- namespace.
        --
        -- The canonical representation is unfortunately broken at the specification
        -- level, as it means entries without a namespace can't exist within ones
        -- which do.
        contextualNamespace =
            maybe [] nameSpace context

        elideNamespace  =
            Text.any (== '.') (baseName name)
                || (List.null contextualNamespace && List.null (nameSpace name))

        nameFields =
            if elideNamespace then
                [ ( "name", encodeText (baseName name) )
                ]

            else
                [ ( "name", encodeText (baseName name) )
                , ( "namespace", encodeText $ Text.intercalate "." (nameSpace name) )
                ]

        encodeAlias a =
            encodeText $
                if nameSpace a == nameSpace name then
                    baseName a

                else
                    Text.intercalate "." (nameSpace a) <> "." <> baseName a

        aliasField =
            if List.null aliases then
                []

            else
                [ ( "aliases", encodeList encodeAlias aliases ) ]
    in
    nameFields <> aliasField


encodeSortOrder :: SortOrder -> Aeson.Value
encodeSortOrder order =
    case order of
        Ascending ->
            Aeson.String "ascending"

        Descending ->
            Aeson.String "descending"

        Ignore ->
            Aeson.String "ignore"


decodeSortOrder :: Aeson.Value -> Aeson.Parser SortOrder
decodeSortOrder = \case
    Aeson.String "ascending" ->
        pure Ascending

    Aeson.String "descending" ->
        pure Descending

    Aeson.String "ignore" ->
        pure Ignore

    _ ->
        fail "Couldn't parse sort order"



encodeList :: (a -> Aeson.Value) -> [ a ] -> Aeson.Value
encodeList f =
    Aeson.Array . Boxed.fromList . fmap f



encodeOptionals :: [( Key, Maybe Aeson.Value )] -> [( Key, Aeson.Value )]
encodeOptionals =
    Maybe.mapMaybe (\( v, s ) -> fmap ( v, ) s)



decodeName :: Maybe TypeName -> Aeson.Object -> Aeson.Parser TypeName
decodeName context obj = do
    name      <- obj Aeson..: "name"
    nameSpace <- obj Aeson..:? "namespace"

    case Name.contextualTypeName context name nameSpace of
        Left s -> fail s
        Right tn -> pure tn


decodeAliases :: TypeName -> Aeson.Object -> Aeson.Parser [TypeName]
decodeAliases context obj = do
    raw <- Aeson.explicitParseFieldMaybe Aeson.parseJSON obj "aliases" Aeson..!= []

    for raw $ \alias ->
        case Name.contextualTypeName (Just context) alias Nothing of
            Left s -> fail s
            Right tn -> pure tn



decodeFields :: Maybe TypeName -> Aeson.Value -> Aeson.Parser Field
decodeFields context (Aeson.Object obj) = do
    fieldName <-
        Aeson.explicitParseField Aeson.parseJSON obj "name"
    fieldAliases <-
        Aeson.explicitParseFieldMaybe Aeson.parseJSON obj "aliases" Aeson..!= []
    fieldDoc <-
        Aeson.explicitParseFieldMaybe Aeson.parseJSON obj "doc"
    fieldOrder <-
        Aeson.explicitParseFieldMaybe decodeSortOrder obj "order"
    fieldType <-
        Aeson.explicitParseField (decodeSchemaInContext context) obj "type"
    fieldDefault <-
        Aeson.explicitParseFieldMaybe' (decodeDefaultValue fieldType) obj "default"

    pure Field {..}

decodeFields _ _ =
    fail "Can't parse Field object"

decodeSchema :: Aeson.Value -> Aeson.Parser Schema
decodeSchema =
    decodeSchemaInContext Nothing


decodeSchemaInContext ::  Maybe TypeName -> Aeson.Value -> Aeson.Parser Schema
decodeSchemaInContext context vs = case vs of
    Aeson.String tag ->
        case tag of
            "null" ->
                pure Null

            "boolean" ->
                pure Boolean

            "int" ->
                pure (Int Nothing)

            "long" ->
                pure (Long Nothing)

            "float" ->
                pure Float

            "double" ->
                pure Double

            "bytes" ->
                pure (Bytes Nothing)

            "string" ->
                pure (String Nothing)

            "array" ->
                fail "Can't parse 'array' as standalone type."

            "map" ->
                fail "Can't parse 'map' as standalone type."

            "record" ->
                fail "Can't parse 'record' as standalone type."

            "fixed" ->
                fail "Can't parse 'fixed' as standalone type."

            "enum" ->
                fail "Can't parse 'enum' as standalone type."

            other ->
                case Name.contextualTypeName context other Nothing of
                    Left s ->
                        fail s
                    Right tn ->
                        pure (NamedType tn)

    Aeson.Object obj ->
        case KeyMap.lookup "type" obj of
            Just (Aeson.String tag) ->
                case tag of
                    "null" ->
                        pure Null

                    "boolean" ->
                        pure Boolean

                    "int" -> do
                        logical <- obj Aeson..:? "logicalType"
                        pure (Int logical)

                    "long" -> do
                        logical <- obj Aeson..:? "logicalType"
                        pure (Long logical)

                    "float" ->
                        pure Float

                    "double" ->
                        pure Double

                    "bytes" -> do
                        logical <- obj Aeson..:? "logicalType"
                        pure (Bytes logical)

                    "string" -> do
                        logical <- obj Aeson..:? "logicalType"
                        pure (String logical)

                    "array" -> do
                        items <-
                            Aeson.explicitParseField Aeson.parseJSON obj "items"

                        pure (Array items)

                    "map" -> do
                        values <-
                            Aeson.explicitParseField Aeson.parseJSON obj "values"

                        pure (Map values)

                    "record" -> do
                        name <-
                            decodeName context obj
                        aliases <-
                            decodeAliases name obj

                        Record name aliases
                            <$> obj Aeson..:? "doc"
                            <*> Aeson.explicitParseField (Aeson.withArray "fields" (traverse (decodeFields context) . Boxed.toList)) obj "fields"

                    "fixed" -> do
                        name <-
                            decodeName context obj
                        aliases <-
                            decodeAliases name obj

                        Fixed name aliases
                            <$> obj Aeson..: "size"
                            <*> obj Aeson..:? "logicalType"

                    "enum" -> do
                        name <-
                            decodeName context obj
                        aliases <-
                            decodeAliases name obj

                        Enum name aliases
                            <$> obj Aeson..: "doc"
                            <*> obj Aeson..: "symbols"
                            <*> obj Aeson..:? "default"

                    other ->
                        case Name.contextualTypeName context other Nothing of
                            Left s ->
                                fail s
                            Right tn ->
                                pure (NamedType tn)



            Just recursive ->
                decodeSchemaInContext context recursive


            Nothing ->
                fail "Type field not found in Schema record"


    Aeson.Array vec ->
        Union <$>
            traverse (decodeSchemaInContext context)
            (Boxed.toList vec)

    _ ->
        fail "Schema was not a list, type name, or object"



encodeDict :: (k -> Key) -> (a -> Aeson.Value) -> Map.Map k a -> Aeson.Value
encodeDict kEnc aEnc vs =
    Aeson.object $
        bimap kEnc aEnc <$>
            Map.toList vs


decodeDict :: (Aeson.Value -> Aeson.Parser a) -> Aeson.Value -> Aeson.Parser (Map.Map Text a)
decodeDict aParser vs =
    case vs of
        Aeson.Object km -> do
            base <-
                traverse aParser km

#if MIN_VERSION_aeson(2,0,0)
            return $
                Map.mapKeysMonotonic Key.toText . KeyMap.toMap $
                    base
#else
            return $
                Map.fromList . KeyMap.toList $
                    base
#endif
        _ ->
            fail "Expected Map"
