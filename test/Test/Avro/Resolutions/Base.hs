module Test.Avro.Resolutions.Base (compatible) where

import qualified Avro
import           Avro.Codec as Codec (Codec (schema))

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import qualified Data.ByteString.Lazy as ByteString

import           Hedgehog

compatible :: (MonadTest m, Eq a, Show a) => Codec a -> Codec b -> a -> b -> m ()
compatible reader writer expect written =
    case Avro.makeDecoder reader (Codec.schema writer) of
        Left schemaError ->
            annotateShow schemaError >> failure
        Right d -> do
            let encoded      = Put.runPut $ Avro.makeEncoder writer written
            (r, _, decoded) <- evalEither $ Get.runGetOrFail d encoded
            assert $ ByteString.null r
            decoded === expect
