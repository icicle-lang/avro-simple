{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Binary where

import           Hedgehog

import qualified Avro
import           Avro.Codec (Codec (..))
import           Avro.Schema (Schema)
import qualified Avro.Value as Avro

import           Test.Avro.Schema.Roundtrip (fuzzSchema, fuzzValue)
import qualified Data.Binary.Put as Put
import qualified Data.Binary.Get as Get
import           Data.Functor.Identity (Identity(..))


fuzzSchemaAndValue :: Gen ( Schema, Avro.Value )
fuzzSchemaAndValue = do
    sch   <- fuzzSchema
    value <- fuzzValue sch
    pure (sch, value)


prop_trip_binary_values :: Property
prop_trip_binary_values =
    withTests 1000 . property $ do
        (sc, example) <- forAll fuzzSchemaAndValue
        let
          dynamic =
            Codec sc Just id

        dec <-
          evalEither $
            Avro.makeDecoder dynamic sc

        tripping example
            (Put.runPut . Avro.makeEncoder dynamic)
            (Identity . Get.runGet dec)



tests :: IO Bool
tests =
  checkParallel $$(discover)
