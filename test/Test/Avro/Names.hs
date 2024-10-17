{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Names where

import           Hedgehog

import           NeatInterpolation (text)

import qualified Data.Aeson as Aeson
import           Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Avro.Schema as Schema
import           Avro.Schema (Schema (..), Field (..))
import           Avro.Name (TypeName(..))


namesExample :: Text
namesExample =
    [text|
    {
      "type": "record",
      "name": "Example",
      "doc": "A simple name (attribute) and no namespace attribute: use the null namespace  the fullname is 'Example'.",
      "fields": [
        {
          "name": "inheritNull",
          "type": {
            "type": "enum",
            "name": "Simple",
            "doc": "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.",
            "symbols": ["a", "b"]
          }
        }, {
          "name": "explicitNamespace",
          "type": {
            "type": "fixed",
            "name": "Simple",
            "namespace": "explicit",
            "doc": "A simple name (attribute) and a namespace (attribute); the fullname is 'explicit.Simple' (this is a different type than of the 'inheritNull' field).",
            "size": 12
          }
        }, {
          "name": "fullName",
          "type": {
            "type": "record",
            "name": "a.full.Name",
            "namespace": "ignored",
            "doc": "A name attribute with a fullname, so the namespace attribute is ignored. The fullname is 'a.full.Name', and the namespace is 'a.full'.",
            "fields": [
              {
                "name": "inheritNamespace",
                "type": {
                  "type": "enum",
                  "name": "Understanding",
                  "doc": "A simple name (attribute) and no namespace attribute: inherit the namespace of the enclosing type 'a.full.Name'. The fullname is 'a.full.Understanding'.",
                  "symbols": ["d", "e"]
                }
              }
            ]
          }
        }
      ]
    }
    |]


namesExpected :: Schema.Schema
namesExpected =
    Record
        (TypeName "Example" [])
        []
        (Just "A simple name (attribute) and no namespace attribute: use the null namespace  the fullname is 'Example'.")
        [ Field {
            fieldName = "inheritNull"
          , fieldDoc = Nothing
          , fieldOrder = Nothing
          , fieldAliases = []
          , fieldDefault = Nothing
          , fieldType =
              Enum
                (TypeName "Simple" [])
                []
                (Just "A simple name (attribute) and no namespace attribute: inherit the null namespace of the enclosing type 'Example'. The fullname is 'Simple'.")
                [ "a", "b" ]
                Nothing
          }
        , Field {
            fieldName = "explicitNamespace"
          , fieldDoc = Nothing
          , fieldOrder = Nothing
          , fieldAliases = []
          , fieldDefault = Nothing
          , fieldType =
              Fixed
                (TypeName "Simple" [ "explicit" ])
                []
                12
                Nothing
          }
        , Field {
            fieldName = "fullName"
          , fieldDoc = Nothing
          , fieldOrder = Nothing
          , fieldAliases = []
          , fieldDefault = Nothing
          , fieldType =
                Record
                    (TypeName "Name" [ "a", "full" ])
                    []
                    (Just "A name attribute with a fullname, so the namespace attribute is ignored. The fullname is 'a.full.Name', and the namespace is 'a.full'.")
                    [ Field {
                        fieldName = "inheritNamespace"
                      , fieldDoc = Nothing
                      , fieldOrder = Nothing
                      , fieldAliases = []
                      , fieldDefault = Nothing
                      , fieldType =
                            Enum
                              (TypeName "Understanding" [ "a", "full" ])
                              []
                              (Just "A simple name (attribute) and no namespace attribute: inherit the namespace of the enclosing type 'a.full.Name'. The fullname is 'a.full.Understanding'.")
                              [ "d", "e" ]
                              Nothing
                      }
                    ]

          }
        ]



-- | This example is kind of pulled from the rust implementation,
--   but, their result is actually incorrect and nests the last
--   long type.

prop_rust_spec_example :: Property
prop_rust_spec_example =
    withTests 1 . property $
      let
        decoded =
            Aeson.eitherDecodeStrict (Text.encodeUtf8 namesExample)
      in
      decoded === Right namesExpected


tests :: IO Bool
tests =
  checkParallel $$(discover)
