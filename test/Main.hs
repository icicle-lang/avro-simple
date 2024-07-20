{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Main (main) where

import           Hedgehog.Main ( defaultMain )

import qualified Test.Avro.Codec.Basics
import qualified Test.Avro.Codec.Compound
import qualified Test.Avro.Codec.Recursive
import qualified Test.Avro.Schema.Roundtrip

main :: IO ()
main =
    defaultMain [
        Test.Avro.Codec.Basics.tests,
        Test.Avro.Codec.Compound.tests,
        Test.Avro.Codec.Recursive.tests,
        Test.Avro.Schema.Roundtrip.tests
    ]
