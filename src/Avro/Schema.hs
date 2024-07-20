{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
module Avro.Schema
    ( Schema(..)
    , Field (..)
    , SortOrder(..)
    , typeName
    , SchemaMismatch(..)
    )
where

import qualified Avro.Name as Name
import           Avro.Name (TypeName (..))
import qualified Avro.Value as Avro

import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Aeson.KeyMap as KeyMap
import qualified Data.Aeson.Key as Key

import           Data.Bifunctor (bimap)
import qualified Data.List as List
import qualified Data.Maybe as Maybe
import qualified Data.Map as Map
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
    | Enum TypeName [TypeName] (Maybe String) [String] (Maybe String)
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


{-| Errors which can occur when trying to read Avro with
an incompatible Schema.
-}
data SchemaMismatch
    = TypeMismatch Schema Schema
    | MissingField TypeName Text
    | FieldMismatch TypeName String SchemaMismatch
    | MissingUnion TypeName
    | MissingSymbol String
    | NamedTypeUnresolved TypeName
    deriving (Eq, Ord, Show)




--
-- JSON codecs
--
-- This isn't written very idiomatically for Aeson, and
-- is a more direct port of the Elm value level code.
--




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
            encodeDict Key.fromText (encodeValue values) ls

        ( Union _, Avro.Union _ Avro.Null ) ->
            Aeson.Null

        ( Union options, Avro.Union ix ls ) ->
            case index ix options of
                Just s ->
                    Aeson.object
                        [ ( Key.fromText (Name.baseName (typeName s)), encodeValue s ls ) ]

                Nothing ->
                    Aeson.Null

        ( Record _ _ _ fields , Avro.Record ls ) ->
            Aeson.object $
                List.zipWith
                    (\f i -> ( Key.fromText (fieldName f), encodeValue (fieldType f) i ))
                    fields
                    ls


        ( Bytes _, Avro.Bytes bytes ) ->
            -- TODO encodeBytes bytes
            Aeson.Null

        ( Fixed {}, Avro.Fixed bytes ) ->
            -- encodeBytes bytes
            Aeson.Null

        _ ->
            Aeson.Null




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


        _ -> Aeson.Null


encodeString :: String -> Aeson.Value
encodeString = Aeson.String . fromString


encodeText :: Text -> Aeson.Value
encodeText = Aeson.String


encodeNameParts :: Maybe TypeName -> TypeName -> [TypeName] -> [ ( Aeson.Key, Aeson.Value ) ]
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



encodeOptionals :: [( Aeson.Key, Maybe Aeson.Value )] -> [( Aeson.Key, Aeson.Value )]
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
    raw <- Aeson.explicitParseFieldOmit (Just []) Aeson.parseJSON obj "aliases"

    for raw $ \alias ->
        case Name.contextualTypeName (Just context) alias Nothing of
            Left s -> fail s
            Right tn -> pure tn




decodeFields :: Maybe TypeName -> Aeson.Value -> Aeson.Parser Field
decodeFields context (Aeson.Object obj) = do
    fieldName <-
        Aeson.explicitParseField Aeson.parseJSON obj "name"
    fieldAliases <-
        Aeson.explicitParseFieldOmit (Just []) Aeson.parseJSON obj "aliases"
    fieldDoc <-
        Aeson.explicitParseFieldMaybe Aeson.parseJSON obj "doc"
    fieldOrder <-
        Aeson.explicitParseFieldMaybe decodeSortOrder obj "order"
    fieldType <-
        Aeson.explicitParseField (decodeSchemaInContext context) obj "type"
    fieldDefault <-
        pure Nothing

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



encodeDict :: (k -> Aeson.Key) -> (a -> Aeson.Value) -> Map.Map k a -> Aeson.Value
encodeDict kEnc aEnc vs =
    Aeson.object $
        bimap kEnc aEnc <$>
            Map.toList vs

