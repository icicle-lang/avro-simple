{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Schema.Roundtrip where

import           Hedgehog
import qualified Hedgehog.Corpus as Corpus
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           NeatInterpolation (text)

import           Avro.Schema (Schema, Field (..), SortOrder (..))
import qualified Avro.Schema as Schema
import qualified Avro.Value as Avro
import           Avro.Name (TypeName(..))

import qualified Data.Aeson as Aeson
import           Data.Function (on)
import qualified Data.List as List
import qualified Data.Map as Map
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text

example1 :: Text.Text
example1 =
    [text|
    {
      "type": "record",
      "name": "test",
      "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
      ]
    }
    |]

example1Expected :: Schema
example1Expected =
    Schema.Record
        (TypeName "test" [])
        []
        Nothing
        [ Field {
            fieldAliases = []
          , fieldDefault = Nothing
          , fieldDoc = Nothing
          , fieldName = "a"
          , fieldOrder = Nothing
          , fieldType = Schema.Long Nothing
          }
        , Field {
            fieldAliases = []
          , fieldDefault = Nothing
          , fieldDoc = Nothing
          , fieldName = "b"
          , fieldOrder = Nothing
          , fieldType = Schema.String Nothing
          }
        ]


prop_test_example_1 :: Property
prop_test_example_1 =
    withTests 1 . property $
        Right example1Expected ===
            Aeson.eitherDecodeStrict (Text.encodeUtf8 example1)


nubSchemas :: [Schema] -> [Schema]
nubSchemas =
    List.nubBy ((==) `on` Schema.typeName)


nubFields  :: [Field] -> [Field]
nubFields  =
    List.nubBy ((==) `on` fieldName)


fuzzBaseName :: Gen Text
fuzzBaseName =
    Gen.element Corpus.boats


fuzzSuits :: Gen [Text]
fuzzSuits = do
    shuffled <- Gen.shuffle [ "CLUBS", "HEARTS", "SPADES", "DIAMONDS" ]
    num <- Gen.int (Range.linear 1 4)
    pure (take num shuffled)


fuzzName :: Gen TypeName
fuzzName =
    TypeName
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 10) (Gen.element Corpus.waters)


flattenUnions :: [Schema] -> [Schema]
flattenUnions =
    let
        unionOptions s =
            case s of
                Schema.Union options ->
                    flattenUnions options

                other ->
                    [ other ]
    in
    List.concatMap unionOptions


fuzzValue :: Schema -> Gen Avro.Value
fuzzValue schema =
    case schema of
        Schema.Null ->
            pure Avro.Null

        Schema.Boolean ->
            Avro.Boolean <$> Gen.bool

        Schema.Int _ ->
            Avro.Int <$> Gen.integral (Range.linear (-10) 10)

        Schema.Long _ ->
            Avro.Long <$> Gen.integral (Range.linear (-10) 10)

        Schema.Float ->
            Avro.Float <$> Gen.float (Range.constant (-10) 10)

        Schema.Double ->
            Avro.Double <$> Gen.double (Range.constant (-10) 10)

        Schema.Bytes _ ->
            Avro.Bytes <$> Gen.bytes (Range.linear 0 10)

        Schema.String _ ->
            Avro.String <$>
                Gen.text (Range.linear 0 10) Gen.ascii

        Schema.Array sc ->
            Avro.Array <$>
                Gen.list (Range.linear 0 10) (fuzzValue sc)

        Schema.Map sc ->
            Avro.Map . Map.fromList <$>
                Gen.list (Range.linear 0 10) (
                    (,) <$> Gen.text (Range.constant 0 10) Gen.ascii
                        <*> fuzzValue sc
                )

        Schema.NamedType _ ->
            Gen.discard

        Schema.Record _ _ _ fis ->
            Avro.Record <$>
                traverse (fuzzValue . fieldType) fis

        Schema.Enum _ _ _ txts _ ->
            Avro.Enum <$>
                Gen.int (Range.linear 0 (length txts - 1))

        Schema.Union [] -> do
            Gen.discard

        Schema.Union scs -> do
            selection <- Gen.int (Range.linear 0 (length scs - 1))
            Avro.Union selection <$>
                fuzzValue (scs !! selection)

        Schema.Fixed _ _ _ size _  ->
            Avro.Fixed <$> Gen.bytes (Range.constant size size)


fuzzDefaultValue :: Schema -> Gen Avro.Value
fuzzDefaultValue schema =
    case schema of
        Schema.Union (x : _) -> do
            Avro.Union 0 <$>
                fuzzValue x

        _ ->
            fuzzValue schema

fuzzField :: Gen Field
fuzzField = do
    schema <- fuzzSchema
    Field
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 2) fuzzBaseName
        <*> pure Nothing
        <*> Gen.maybe (Gen.element [ Ascending, Descending, Ignore ])
        <*> pure schema
        <*> Gen.maybe (fuzzDefaultValue schema)


fuzzSchema :: Gen Schema
fuzzSchema =
    Gen.recursive Gen.choice
        [ pure Schema.Null
        , pure Schema.Boolean
        , Schema.Int
              <$> Gen.maybe (pure <$> Gen.ascii)
        , Schema.Long
              <$> Gen.maybe (pure <$> Gen.ascii)
        , pure Schema.Float
        , pure Schema.Double
        , Schema.Bytes
              <$> Gen.maybe (pure <$> Gen.ascii)
        , Schema.Enum
              <$> fuzzName
              <*> Gen.list (Range.linear 0 5) fuzzName
              <*> Gen.maybe (pure <$> Gen.ascii)
              <*> fuzzSuits
              <*> pure Nothing
        , Schema.Fixed
              <$> fuzzName
              <*> Gen.list (Range.linear 0 5) fuzzName
              <*> Gen.maybe (pure <$> Gen.ascii)
              <*> Gen.int (Range.linear 0 20)
              <*> Gen.maybe (pure <$> Gen.ascii)
        , Schema.String
              <$> Gen.maybe (pure <$> Gen.ascii)
        ]

        [ Schema.Array
              <$> fuzzSchema
        , Schema.Map
              <$> fuzzSchema
        , Schema.Record
              <$> fuzzName
              <*> Gen.list (Range.linear 0 5) fuzzName
              <*> Gen.maybe (pure <$> Gen.ascii)
              <*> (nubFields <$> Gen.list (Range.linear 1 5) fuzzField)
        , Schema.Union . nubSchemas . flattenUnions
              <$> Gen.list (Range.linear 1 5) fuzzSchema
        ]


prop_trip_schema_aeson :: Property
prop_trip_schema_aeson =
    withTests 1000 . property $ do
        schema <- forAll fuzzSchema
        tripping
            schema
            Aeson.encode
            Aeson.eitherDecode



tests :: IO Bool
tests =
  checkParallel $$(discover)
