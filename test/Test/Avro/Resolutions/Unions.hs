{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Resolutions.Unions  where

import qualified Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import           Data.Text (Text)

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import           Data.Int (Int64, Int32)
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


largeNumber :: Int64
largeNumber =
    914793674309632



prop_int_to_option_int :: Property
prop_int_to_option_int =
    withTests 1 . property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible (Codec.maybe Codec.int) Codec.int (Just example) example


prop_int_to_option_long :: Property
prop_int_to_option_long =
    withTests 1 . property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible (Codec.maybe Codec.int64) Codec.int (Just (fromIntegral example)) example


prop_int_long_on_left :: Property
prop_int_long_on_left =
    withTests 1 . property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible (Codec.union Codec.int64 Codec.int) Codec.int (Right example) example


prop_int_long_on_right :: Property
prop_int_long_on_right =
    withTests 1 . property $ do
        example <- forAll $ Gen.integral (Range.linear minBound maxBound)
        compatible (Codec.union Codec.int Codec.int64) Codec.int (Left example) example


prop_person_compatibility_from_single :: Property
prop_person_compatibility_from_single =
    withTests 1 . property $ do
        compatible (Codec.maybe version2) version1 (Just (Person "Francis" Nothing)) (Person "Francis" Nothing)


prop_person_compatibility_from_union :: Property
prop_person_compatibility_from_union =
    withTests 1 . property $ do
        compatible (Codec.maybe version2) (Codec.maybe version1) (Just (Person "Francis" Nothing)) (Just (Person "Francis" Nothing))




tests :: IO Bool
tests =
  checkParallel $$(discover)
