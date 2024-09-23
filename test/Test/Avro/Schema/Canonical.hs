{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Schema.Canonical where

import           Hedgehog

import           NeatInterpolation (text)

import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as ByteString
import           Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Avro.Schema as Avro


testExample :: MonadTest m => Text -> Text -> m ()
testExample expanded canonical =
    let
        decoded =
            Aeson.eitherDecodeStrict (Text.encodeUtf8 expanded)

        encoded =
            ByteString.toStrict . Avro.renderCanonical <$> decoded
    in
    encoded === Right (Text.encodeUtf8 canonical)


-- | This example is kind of pulled from the rust implementation,
--   but, their result is actually incorrect and nests the last
--   long type.


prop_rust_spec_example :: Property
prop_rust_spec_example =
    let
        expanded =
            [text|
            {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42, "doc": "The field a"},
                {"name": "b", "type": "string", "namespace": "test.a"},
                {"name": "c", "type": "long", "logicalType": "timestamp-micros"}
            ]
            }
            |]

        canonical =
            [text|{"name":"test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"long"}]}|]
    in
    withTests 1 . property $
        testExample expanded canonical


prop_example_00 :: Property
prop_example_00 =
    let
        expanded =
            [text|"null"|]
        canonical =
            [text|"null"|]
    in
    withTests 1 . property $
        testExample expanded canonical


prop_example_01 :: Property
prop_example_01 =
    let
        expanded =
            [text|{"type":"null"}|]
        canonical =
            [text|"null"|]
    in
    withTests 1 . property $
        testExample expanded canonical


prop_example_02 :: Property
prop_example_02 =
    let
        expanded =
            [text|"boolean"|]
        canonical =
            [text|"boolean"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_03 :: Property
prop_example_03 =
    let
        expanded =
            [text| {"type":"boolean"}|]
        canonical =
            [text|"boolean"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_04 :: Property
prop_example_04 =
    let
        expanded =
            [text|"int"|]
        canonical =
            [text|"int"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_05 :: Property
prop_example_05 =
    let
        expanded =
            [text|{"type":"int"}|]
        canonical =
            [text|"int"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_06 :: Property
prop_example_06 =
    let
        expanded =
            [text|"long"|]
        canonical =
            [text|"long"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_07 :: Property
prop_example_07 =
    let
        expanded =
            [text|{"type":"long"}|]
        canonical =
            [text|"long"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_08 :: Property
prop_example_08 =
    let
        expanded =
            [text|"float"|]
        canonical =
            [text|"float"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_09 :: Property
prop_example_09 =
    let
        expanded =
            [text|{"type":"float"}|]
        canonical =
            [text|"float"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_10 :: Property
prop_example_10 =
    let
        expanded =
            [text|"double"|]
        canonical =
            [text|"double"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_11 :: Property
prop_example_11 =
    let
        expanded =
            [text|{"type":"double"}|]
        canonical =
            [text|"double"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_12 :: Property
prop_example_12 =
    let
        expanded =
            [text|"bytes"|]
        canonical =
            [text|"bytes"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_13 :: Property
prop_example_13 =
    let
        expanded =
            [text|{"type":"bytes"}|]
        canonical =
            [text|"bytes"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_14 :: Property
prop_example_14 =
    let
        expanded =
            [text|"string"|]
        canonical =
            [text|"string"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_15 :: Property
prop_example_15 =
    let
        expanded =
            [text|{"type":"string"}|]
        canonical =
            [text|"string"|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_16 :: Property
prop_example_16 =
    let
        expanded =
            "[  ]"
        canonical =
            "[]"
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_17 :: Property
prop_example_17 =
    let
        expanded =
            [text|[ "int"  ]|]
        canonical =
            [text|["int"]|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_18 :: Property
prop_example_18 =
    let
        expanded =
            [text|[ "int" , {"type":"boolean"} ]|]
        canonical =
            [text|["int","boolean"]|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_19 :: Property
prop_example_19 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"foo"}|]
        canonical =
            [text|{"name":"foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_20 :: Property
prop_example_20 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}|]
        canonical =
            [text|{"name":"x.y.foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_21 :: Property
prop_example_21 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}|]
        canonical =
            [text|{"name":"a.b.foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_22 :: Property
prop_example_22 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}|]
        canonical =
            [text|{"name":"foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_23 :: Property
prop_example_23 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}|]
        canonical =
            [text|{"name":"foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_24 :: Property
prop_example_24 =
    let
        expanded =
            [text|{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}|]
        canonical =
            [text|{"name":"foo","type":"record","fields":[]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_25 :: Property
prop_example_25 =
    let
        expanded =
            [text|{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}|]
        canonical =
            [text|{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_26 :: Property
prop_example_26 =
    let
        expanded =
            [text|
            {
                "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true},
                {"order":"descending","name":"f2","doc":"Hello","type":"int"}],
                "type":"record", "name":"foo"
                }
            |]
        canonical =
            [text|{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"},{"name":"f2","type":"int"}]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_27 :: Property
prop_example_27 =
    let
        expanded =
            [text|{"type":"enum", "name":"foo", "symbols":["A1"]}|]
        canonical =
            [text|{"name":"foo","type":"enum","symbols":["A1"]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_28 :: Property
prop_example_28 =
    let
        expanded =
            [text|{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}|]
        canonical =
            [text|{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_29 :: Property
prop_example_29 =
    let
        expanded =
            [text|{"name":"foo","type":"fixed","size":15}|]
        canonical =
            [text|{"name":"foo","type":"fixed","size":15}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_30 :: Property
prop_example_30 =
    let
        expanded =
            [text|{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}|]
        canonical =
            [text|{"name":"x.y.z.foo","type":"fixed","size":32}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_31 :: Property
prop_example_31 =
    let
        expanded =
            [text|{ "items":{"type":"null"}, "type":"array"}|]
        canonical =
            [text|{"type":"array","items":"null"}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_32 :: Property
prop_example_32 =
    let
        expanded =
            [text|{ "values":"string", "type":"map"}|]
        canonical =
            [text|{"type":"map","values":"string"}|]
    in
    withTests 1 . property $
        testExample expanded canonical



prop_example_33 :: Property
prop_example_33 =
    let
        expanded =
            [text|
            {"name":"PigValue","type":"record",
            "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}
            |]
        canonical =
            [text|{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}|]
    in
    withTests 1 . property $
        testExample expanded canonical


tests :: IO Bool
tests =
  checkParallel $$(discover)
