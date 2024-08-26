{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Codec.Basics where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import qualified Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import           Data.Functor.Identity (Identity (..))
import qualified Data.Map as Map


trip :: (Eq a, Show a) => Codec a -> a -> PropertyT IO ()
trip codec =
    tripVersions codec codec

tripVersions :: (Eq a, Show a) => Codec a -> Codec a -> a -> PropertyT IO ()
tripVersions reader writer example = do
    decoder <-
        evalEither $
            Avro.makeDecoder reader (Codec.schema writer)

    tripping example
        (Put.runPut . Avro.makeEncoder writer)
        (Identity . Get.runGet decoder)



prop_null_codec :: Property
prop_null_codec =
    withTests 100 . property $ do
        trip Codec.unit ()

prop_bool_codec :: Property
prop_bool_codec =
    withTests 100 . property $ do
        example <- forAll Gen.bool
        trip Codec.bool example

prop_int_codec :: Property
prop_int_codec =
    withTests 100 . property $ do
        example <- forAll $ Gen.integral (Range.linear (-1000) 1000)
        trip Codec.int example

prop_long_codec :: Property
prop_long_codec =
    withTests 100 . property $ do
        example <- forAll $ Gen.integral (Range.linear (-1000) 1000)
        trip Codec.int64 example

prop_float_codec :: Property
prop_float_codec =
    withTests 100 . property $ do
        example <- forAll $ Gen.float (Range.constant (-1000) 1000)
        trip Codec.float example

prop_double_codec :: Property
prop_double_codec =
    withTests 100 . property $ do
        example <- forAll $ Gen.double (Range.constant (-1000) 1000)
        trip Codec.double example

prop_string_codec :: Property
prop_string_codec =
    withTests 100 . property $ do
        example <- forAll $ Gen.text (Range.linear 0 100) Gen.ascii
        trip Codec.string example

prop_array_of_string_codec :: Property
prop_array_of_string_codec =
    withTests 100 . property $ do
        example <-
            forAll $
                Gen.list (Range.linear 0 10) $
                Gen.text (Range.linear 0 100) Gen.ascii

        trip (Codec.array Codec.string) example

prop_array_of_array_of_string_codec :: Property
prop_array_of_array_of_string_codec =
    withTests 100 . property $ do
        example <-
            forAll $
                Gen.list (Range.linear 0 5) $
                Gen.list (Range.linear 0 5) $
                Gen.text (Range.linear 0 10) Gen.ascii

        trip (Codec.array (Codec.array Codec.string)) example

prop_map_of_string_codec :: Property
prop_map_of_string_codec =
    withTests 100 . property $ do
        example <-
            forAll $
                Gen.list (Range.linear 0 10) $
                (,) <$> Gen.text (Range.linear 0 100) Gen.ascii
                    <*> Gen.text (Range.linear 0 100) Gen.ascii

        trip (Codec.dict Codec.string) (Map.fromList example)


tests :: IO Bool
tests =
  checkParallel $$(discover)
