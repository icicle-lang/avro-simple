{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec

import           Gauge
import           Avro.Name (TypeName(..))

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import           Data.Text (Text)
import           Data.Int (Int32)


data Person =
    Person {
        personWho :: Text
      , personAge :: Int32
      , personSex :: Maybe Text
      , personAccounts :: [Account]
    } deriving (Eq, Show)


data Account =
    Account {
        accountId :: Int32
      , accountKind :: Maybe Text
      , accountCosignatories :: [Person]
    } deriving (Eq, Show)


accountCodec :: Codec Account
accountCodec =
    Codec.record (TypeName "account" []) $
        Account
            <$> Codec.requiredField "id" Codec.int accountId
            <*> Codec.optionalField "kind" Codec.string accountKind
            <*> Codec.requiredField "cosignatories" (Codec.array (Codec.namedType personCodec)) accountCosignatories


personCodec :: Codec Person
personCodec =
    Codec.record (TypeName "person" []) $
        Person
            <$> Codec.requiredField "name" Codec.string personWho
            <*> Codec.requiredField "age" Codec.int personAge
            <*> Codec.optionalField "sex" Codec.string personSex
            <*> Codec.fallbackField "degrees" (Codec.array accountCodec) [] personAccounts


basicilio :: Person
basicilio =
    Person "Basicilio" 84 Nothing []


fredericulio :: Person
fredericulio =
    Person "Fredericulio" 34 (Just "rather not say") []


juglidrio :: Person
juglidrio =
    Person "Juglidrio" 52 Nothing [ Account 4 Nothing [ basicilio ], Account 10 (Just "Bankers") [ fredericulio ] ]


main :: IO ()
main = do
    let
        !juglidrioEncoded =
            Put.runPut (Avro.makeEncoder personCodec juglidrio)
        Right !decoder =
            Avro.makeDecoder personCodec (Codec.schema personCodec)

    defaultMain
        [ bgroup "Encoding Juglidrio"
            [ bench "Read" $ whnf (Put.runPut . Avro.makeEncoder personCodec) juglidrio
            ]
        , bgroup "Decoding Juglidrio"
            [ bench "Read" $ whnf (Get.runGet decoder) juglidrioEncoded
            ]
        ]
