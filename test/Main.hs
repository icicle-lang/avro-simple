{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Main (main) where

import           Hedgehog.Main ( defaultMain )

import qualified Test.Avro.Binary
import qualified Test.Avro.Codec.Basics
import qualified Test.Avro.Codec.Compound
import qualified Test.Avro.Codec.Recursive
import qualified Test.Avro.Schema.Roundtrip
import qualified Test.Avro.Schema.Validation
import qualified Test.Avro.Resolutions.Basics
import qualified Test.Avro.Resolutions.Unions

main :: IO ()
main =
    defaultMain [
        Test.Avro.Binary.tests,
        Test.Avro.Codec.Basics.tests,
        Test.Avro.Codec.Compound.tests,
        Test.Avro.Codec.Recursive.tests,
        Test.Avro.Schema.Roundtrip.tests,
        Test.Avro.Schema.Validation.tests,
        Test.Avro.Resolutions.Basics.tests,
        Test.Avro.Resolutions.Unions.tests
    ]
