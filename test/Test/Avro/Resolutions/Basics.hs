{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Resolutions.Basics  where

import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec

import           Data.Text (Text)

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import           Data.Int (Int32)
import           Avro.Name (TypeName(..))
import           Test.Avro.Resolutions.Base (compatible)

data Person =
    Person
      { name :: Text
      , age :: Maybe Int32
      } deriving (Eq, Show)


version1 :: Codec Person
version1 =
    Codec.record (TypeName "Person" []) $ Person
          <$> Codec.requiredField "name" Codec.string name
          <*> pure Nothing


version2 :: Codec Person
version2 =
    Codec.record (TypeName "PersonV2" []) $ Person
          <$> Codec.requiredField "name" Codec.string name
          <*> Codec.optionalField "age" Codec.int age


prop_int_to_long :: Property
prop_int_to_long =
    property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible Codec.int64 Codec.int (fromIntegral example) example

prop_int_to_float :: Property
prop_int_to_float =
    property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible Codec.float Codec.int (fromIntegral example) example

prop_int_to_double :: Property
prop_int_to_double =
    property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible Codec.float Codec.int (fromIntegral example) example

prop_long_to_float :: Property
prop_long_to_float =
    property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible Codec.float Codec.int64 (fromIntegral example) example

prop_long_to_double :: Property
prop_long_to_double =
    property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible Codec.float Codec.int64 (fromIntegral example) example



prop_person_compatibility :: Property
prop_person_compatibility =
    withTests 1 . property $ do
        compatible version2 version1 (Person "Francis" Nothing) (Person "Francis" Nothing)


tests :: IO Bool
tests =
  checkParallel $$(discover)
