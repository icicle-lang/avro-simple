{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Schema.Validation where

import           Hedgehog
import           Avro.Schema (Schema, Field (..))
import qualified Avro.Schema as Schema
import           Avro.Name (TypeName(..))


nestedUnion :: Schema
nestedUnion =
    Schema.Union
        [ Schema.Union [] ]



duplicateUnion :: Schema
duplicateUnion =
    Schema.Union
        [ Schema.Double, Schema.Double ]


goodEnum :: Schema
goodEnum =
    Schema.Enum
        (TypeName "ok" [])
        []
        Nothing
        [ "a", "b" ]
        Nothing



nameRedefined :: Schema
nameRedefined =
    Schema.Record
        (TypeName "something" [])
        []
        Nothing
        [ Field { fieldName = "a", fieldAliases = [], fieldDoc = Nothing, fieldOrder = Nothing, fieldType = goodEnum, fieldDefault = Nothing }
        , Field{ fieldName = "b", fieldAliases = [], fieldDoc = Nothing, fieldOrder = Nothing, fieldType = goodEnum, fieldDefault = Nothing }
        ]



shouldFail :: MonadFail f => Schema -> f ()
shouldFail schema =
  case Schema.validateSchema schema of
    Left _ -> pure ()
    Right _ -> fail "Expected error"



prop_nestedUnion :: Property
prop_nestedUnion =
    withTests 1 . property $
        shouldFail nestedUnion


prop_duplicateUnion :: Property
prop_duplicateUnion =
    withTests 1 . property $
        shouldFail duplicateUnion



prop_goodEnum :: Property
prop_goodEnum =
    withTests 1 . property $
        evalEither (Schema.validateSchema goodEnum)


prop_nameRedefined :: Property
prop_nameRedefined =
    withTests 1 . property $
        shouldFail nameRedefined



tests :: IO Bool
tests =
  checkParallel $$(discover)
