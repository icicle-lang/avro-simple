{-# LANGUAGE QuasiQuotes, OverloadedStrings, TemplateHaskell #-}
module Test.Avro.Codec.Compound where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import qualified Avro
import           Avro.Codec (Codec)
import qualified Avro.Codec as Codec
import           Avro.Name (TypeName (..))

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import           Data.Functor.Identity (Identity (..))
import           Data.Text (Text)
import           Data.Int (Int32)



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


newtype Degree =
    Degree {
        degreeName :: Text
    } deriving (Eq, Show)


data Student =
    Student {
        studentName :: Text
      , studentAge :: Int32
      , studentSex :: Maybe Text
      , studentDegrees :: [Degree]
    } deriving (Eq, Show)

data Staff =
    Staff {
        staffName :: Text
      , staffSchool :: Maybe Text
    } deriving (Eq, Show)


degreeCodec :: Codec Degree
degreeCodec =
    Codec.record (TypeName "degree" []) $ Degree
          <$> Codec.requiredField "name" Codec.string degreeName


studentCodec :: Codec Student
studentCodec =
    Codec.record (TypeName "student" []) $ Student
          <$> Codec.requiredField "name" Codec.string studentName
          <*> Codec.requiredField "age" Codec.int studentAge
          <*> Codec.optionalField "sex" Codec.string studentSex
          <*> Codec.fallbackField "degrees" (Codec.array degreeCodec) [] studentDegrees


staffCodec :: Codec Staff
staffCodec =
    Codec.record (TypeName "staff" []) $ Staff
          <$> Codec.requiredField "name" Codec.string staffName
          <*> Codec.optionalField "school" Codec.string staffSchool


studentOrStaff :: Codec (Either Student Staff)
studentOrStaff =
    Codec.union studentCodec staffCodec


fuzzStudent :: Gen Student
fuzzStudent =
    Student
        <$> Gen.text (Range.linear 0 100) Gen.ascii
        <*> Gen.integral (Range.linear 18 100)
        <*> Gen.maybe (Gen.element ["M", "F"])
        <*> Gen.list (Range.linear 0 4)
                (Degree <$> Gen.text (Range.linear 0 100) Gen.ascii)


fuzzStaff :: Gen Staff
fuzzStaff =
    Staff
        <$> Gen.text (Range.linear 0 100) Gen.ascii
        <*> Gen.maybe (Gen.text (Range.linear 0 100) Gen.ascii)


prop_student_codec :: Property
prop_student_codec =
    withTests 100 . property $ do
        example <- forAll fuzzStudent
        trip studentCodec example


prop_staff_codec :: Property
prop_staff_codec =
    withTests 100 . property $ do
        example <- forAll fuzzStaff
        trip staffCodec example


prop_union_codec :: Property
prop_union_codec =
    withTests 100 . property $ do
        example <- forAll (Gen.choice [Left <$> fuzzStudent, Right <$> fuzzStaff])
        trip studentOrStaff example


tests :: IO Bool
tests =
  checkParallel $$(discover)
