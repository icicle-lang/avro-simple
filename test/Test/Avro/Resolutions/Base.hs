module Test.Avro.Resolutions.Base (compatible) where

import qualified Avro
import           Avro.Codec as Codec (Codec (schema))

import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put

import           Hedgehog

compatible :: (MonadTest m, Eq a, Show a) => Codec a -> Codec b -> a -> b -> m ()
compatible reader writer expect written =
    case Avro.makeDecoder reader (Codec.schema writer) of
        Left schemaError ->
            annotateShow schemaError >> failure
        Right d -> do
            let encoded = Put.runPut $ Avro.makeEncoder writer written
            let decoded = Get.runGet d encoded
            decoded === expect
