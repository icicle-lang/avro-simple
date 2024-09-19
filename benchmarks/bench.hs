{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec
import           Avro.Name (TypeName(..))

import           Gauge

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


linkedList :: Codec a -> Codec [a]
linkedList a =
    let
        packList (Just (g, b)) =
            g : b

        packList Nothing=
            []

        unpackList (g:b) =
            Just (g, b)

        unpackList [] =
            Nothing

        builder recur =
            (,) <$> Codec.requiredField "head" a fst
                <*> Codec.requiredField "tail" (reconfigure recur) snd

        consCodec =
              Codec.recursiveRecord (TypeName "cons" []) builder

        reconfigure =
            Codec.invmap packList unpackList . Codec.maybe

    in
    reconfigure
          consCodec


linkedInts :: Codec [Int32]
linkedInts =
    linkedList Codec.int



main :: IO ()
main = do
    let
        !juglidrioEncoded =
            Put.runPut (Avro.makeEncoder personCodec juglidrio)
        Right !decoder =
            Avro.makeDecoder personCodec (Codec.schema personCodec)

        listEncoder =
            Avro.makeEncoder linkedInts

        Right !listDecoder =
            Avro.makeDecoder linkedInts (Codec.schema linkedInts)

        !twoEncoded =
            Put.runPut $ listEncoder [1, 2]

        !tenEncoded =
            Put.runPut $ listEncoder [1..10]

        !fiftyEncoded =
            Put.runPut $ listEncoder [1..50]

        arrayEncoder =
            Avro.makeEncoder (Codec.array Codec.int)

        Right !arrayDecoder =
            Avro.makeDecoder (Codec.array Codec.int) (Codec.schema (Codec.array Codec.int))

        !twoArrEncoded =
            Put.runPut $ arrayEncoder [1, 2]

        !tenArrEncoded =
            Put.runPut $ arrayEncoder [1..10]

        !fiftyArrEncoded =
            Put.runPut $ arrayEncoder [1..50]


    defaultMain
        [ bgroup "Person Codec"
            [ bench "Write" $ whnf (Put.runPut . Avro.makeEncoder personCodec) juglidrio
            , bench "Read" $ whnf (Get.runGet decoder) juglidrioEncoded
            ]

        , bgroup "Linked List"
            [ bench "Size 2" $ whnf (Get.runGet listDecoder) twoEncoded
            , bench "Size 10" $ whnf (Get.runGet listDecoder) tenEncoded
            , bench "Size 50" $ whnf (Get.runGet listDecoder) fiftyEncoded
            ]

        , bgroup "Array List"
            [ bench "Size 2" $ whnf (Get.runGet arrayDecoder) twoArrEncoded
            , bench "Size 10" $ whnf (Get.runGet arrayDecoder) tenArrEncoded
            , bench "Size 50" $ whnf (Get.runGet arrayDecoder) fiftyArrEncoded
            ]
        ]
