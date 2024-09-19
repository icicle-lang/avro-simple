{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Codec.Recursive where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import qualified Avro
import           Avro.Codec (Codec, invmap)
import qualified Avro.Codec as Codec
import           Avro.Name (TypeName(..))

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import           Data.Functor.Identity (Identity (..))
import           Data.Int (Int64)


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
            invmap packList unpackList . Codec.maybe

    in
    reconfigure
          consCodec


prop_recursive_linked_list :: Property
prop_recursive_linked_list =
    withTests 100 . property $ do
        example <-
            forAll $
                Gen.list (Range.linear 0 100) $
                Gen.text (Range.linear 0 100) Gen.ascii

        trip (linkedList Codec.string) example


data LinkedList a =
    LinkedList {
        long :: a,
        rest :: Maybe (LinkedList a)
    } deriving (Eq, Show)


linkedLongs :: Codec (LinkedList Int64)
linkedLongs =
    Codec.recursiveRecord (TypeName "LinkedLongs" []) $ \rec ->
        LinkedList
            <$> Codec.requiredField "long" Codec.int64 long
            <*> Codec.optionalField "rest" rec rest


prop_custom_linked_list :: Property
prop_custom_linked_list =
    withTests 100 . property $ do
        top <-
            forAll $
                Gen.int64 (Range.linear 0 10)

        more <-
            forAll $
                Gen.list (Range.linear 0 100) $
                Gen.int64 (Range.linear 0 10)

        let
            example =
                LinkedList top
                    $ foldr (\a b -> Just (LinkedList a b)) Nothing more


        trip linkedLongs example


tests :: IO Bool
tests =
  checkParallel $$(discover)
