{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Resolutions.Enumerations  where

import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec

import           Data.Text (Text)

import           Hedgehog
import           Data.Int (Int32)
import           Avro.Name (TypeName(..))
import           Test.Avro.Resolutions.Base (compatible)

data Person =
    Person
      { name :: Text
      , age :: Maybe Int32
      } deriving (Eq, Show)


version1 :: Codec Int
version1 =
    Codec.enum (TypeName "myEnum" [ "n1" ]) [ "e1", "e3", "e4" ] Nothing


version2 :: Codec Int
version2 =
    Codec.enum (TypeName "myEnum" [ "n1" ]) [ "e1", "e2", "e3" ] (Just "e2")


prop_enum_full_match :: Property
prop_enum_full_match =
    withTests 1 . property $ do
        compatible version2 version1 0 0


prop_enum_shift_match :: Property
prop_enum_shift_match =
    withTests 1 . property $ do
        compatible version2 version1 1 2


prop_removed_as_default :: Property
prop_removed_as_default =
    withTests 1 . property $ do
        compatible version2 version1 2 1


tests :: IO Bool
tests =
  checkParallel $$(discover)
