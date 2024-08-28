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
        packList (Right (g, b)) =
            g : b

        packList (Left ())=
            []

        unpackList (g:b) =
            Right (g, b)

        unpackList [] =
            Left ()

        builder =
            (,) <$> Codec.requiredField "head" a fst
                <*> Codec.requiredField "tail" (packedCodec (Codec.namedType consCodec)) snd

        consCodec =
              Codec.record (TypeName "cons" []) builder

        packedCodec consCodec' =
            invmap packList unpackList $
                Codec.union
                    Codec.unit
                    consCodec'

    in
    packedCodec
          consCodec


prop_recursive_linked_list :: Property
prop_recursive_linked_list =
    withTests 100 . property $ do
        example <-
            forAll $
                Gen.list (Range.linear 0 100) $
                Gen.text (Range.linear 0 100) Gen.ascii

        trip (linkedList Codec.string) example


tests :: IO Bool
tests =
  checkParallel $$(discover)
