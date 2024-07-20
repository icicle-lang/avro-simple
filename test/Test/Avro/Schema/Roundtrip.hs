{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Schema.Roundtrip where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           NeatInterpolation (text)

import           Avro.Schema (Schema, Field (..), SortOrder (..))
import qualified Avro.Schema as Schema
import           Avro.Name (TypeName(..))

import           Control.Applicative
import qualified Data.Aeson as Aeson
import           Data.Function (on)
import qualified Data.List as List
import           Data.Text (Text)
import qualified Data.Text as Text

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
            Aeson.eitherDecodeStrictText example1


numSchemas :: [Schema] -> [Schema]
numSchemas =
    List.nubBy ((==) `on` Schema.typeName)


fuzzBaseName :: Gen Text
fuzzBaseName =
    Gen.element [ "foo", "bar", "baz" ]


fuzzName :: Gen TypeName
fuzzName =
    TypeName
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 10) fuzzBaseName


fuzzField :: Gen Field
fuzzField =
    Field
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 10) fuzzBaseName
        <*> pure Nothing
        <*> Gen.maybe (Gen.element [ Ascending, Descending, Ignore ])
        <*> fuzzSchema
        <*> pure Nothing


fuzzSchema :: Gen Schema
fuzzSchema =
    Gen.recursive Gen.choice
        [ pure Schema.Null
        , pure Schema.Boolean
        , Schema.Int
              <$> optional (pure <$> Gen.ascii)
        , Schema.Long
              <$> optional (pure <$> Gen.ascii)
        , pure Schema.Float
        , pure Schema.Double
        , Schema.Bytes
              <$> optional (pure <$> Gen.ascii)
        , Schema.String
              <$> optional (pure <$> Gen.ascii)
        ]

        [ Schema.Array
              <$> fuzzSchema
        , Schema.Map
              <$> fuzzSchema
        , Schema.Record
              <$> fuzzName
              <*> Gen.list (Range.linear 0 5) fuzzName
              <*> optional (pure <$> Gen.ascii)
              <*> Gen.list (Range.linear 1 5) fuzzField
        , Schema.Union . numSchemas
              <$> Gen.list (Range.linear 1 5) fuzzSchema
        ]


prop_round_trip :: Property
prop_round_trip =
    withTests 200 . property $ do
        schema <- forAll fuzzSchema
        tripping
            schema
            Aeson.encode
            Aeson.decode



tests :: IO Bool
tests =
  checkParallel $$(discover)
