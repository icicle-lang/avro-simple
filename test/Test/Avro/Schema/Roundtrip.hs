{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Schema.Roundtrip where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           NeatInterpolation (text)

import           Avro.Schema (Schema, Field (..), SortOrder (..))
import qualified Avro.Schema as Schema
import qualified Avro.Value as Avro
import           Avro.Name (TypeName(..))

import           Control.Applicative
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


numSchemas :: [Schema] -> [Schema]
numSchemas =
    List.nubBy ((==) `on` Schema.typeName)


nubFields  :: [Field] -> [Field]
nubFields  =
    List.nubBy ((==) `on` fieldName)


fuzzBaseName :: Gen Text
fuzzBaseName =
    Gen.element [ "foo", "bar", "baz" ]


fuzzName :: Gen TypeName
fuzzName =
    TypeName
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 10) fuzzBaseName


fuzzValue :: Schema -> Gen Avro.Value
fuzzValue schema =
    case schema of
        Schema.Null ->
            pure Avro.Null

        Schema.Boolean ->
            Avro.Boolean <$> Gen.bool

        Schema.Int m_s ->
            Avro.Int <$> Gen.integral (Range.linear (-10) 10)

        Schema.Long m_s ->
            Avro.Long <$> Gen.integral (Range.linear (-10) 10)

        Schema.Float ->
            Avro.Float <$> Gen.float (Range.constant (-10) 10)

        Schema.Double ->
            Avro.Double <$> Gen.double (Range.constant (-10) 10)

        Schema.Bytes m_s ->
            Gen.discard

        Schema.String m_s ->
            Avro.String <$>
                Gen.text (Range.constant (-10) 10) Gen.ascii

        Schema.Array sc ->
            Avro.Array <$>
                Gen.list (Range.linear 0 10) (fuzzValue sc)

        Schema.Map sc ->
            Avro.Map . Map.fromList <$>
                Gen.list (Range.linear 0 10) (
                    (,) <$> Gen.text (Range.constant 0 10) Gen.ascii
                        <*> fuzzValue sc
                )

        Schema.NamedType tn ->
            Gen.discard

        Schema.Record tn tns m_s fis ->
            Avro.Record <$>
                traverse (fuzzValue . fieldType) fis

        Schema.Enum tn tns m_s txts m_txt ->
            Avro.Enum <$>
                Gen.int (Range.linear 0 (length txts - 1))

        Schema.Union [] -> do
            Gen.discard

        Schema.Union scs -> do
            selection <- Gen.int (Range.linear 0 (length scs - 1))
            Avro.Union selection <$>
                fuzzValue (scs !! selection)

        Schema.Fixed tn tns n m_s ->
            Gen.discard


fuzzDefaultValue :: Schema -> Gen Avro.Value
fuzzDefaultValue schema =
    case schema of
        Schema.Union (x : xs) -> do
            Avro.Union 0 <$>
                fuzzValue x

        _ ->
            fuzzValue schema

fuzzField :: Gen Field
fuzzField = do
    schema <- fuzzSchema
    Field
        <$> fuzzBaseName
        <*> Gen.list (Range.linear 0 10) fuzzBaseName
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
              <*> (nubFields <$> Gen.list (Range.linear 1 5) fuzzField)
        , Schema.Union . numSchemas
              <$> Gen.list (Range.linear 1 5) fuzzSchema
        ]


prop_round_trip :: Property
prop_round_trip =
    withTests 1000 . property $ do
        schema <- forAll fuzzSchema
        tripping
            schema
            Aeson.encode
            Aeson.eitherDecode



tests :: IO Bool
tests =
  checkParallel $$(discover)
