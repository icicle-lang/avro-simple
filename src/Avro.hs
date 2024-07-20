module Avro
    ( makeDecoder, makeEncoder
    ) where

import qualified Avro.Codec as Codec
import qualified Avro.Internal.Bytes as Bytes
import           Avro.Internal.Deconflict (deconflict)
import           Avro.Schema (Schema, SchemaMismatch)
import           Data.Binary (Get, Put)
import qualified Data.Set as Set

{-| Create a binary decoder for avro data given a Codec and the writer's Schema.

Fields in Avro data types are not tagged, and records can only be interpreted
knowing the exact Schema with which they were written.

Therefore, building a binary decoder not requires a Codec for the type of
interest, but also the writer of the data's [`Schema`](Avro-Schema#Schema).

-}
makeDecoder :: Codec.Codec a -> Schema -> Either SchemaMismatch (Get a)
makeDecoder codec writerSchema = do
    readSchema <- deconflict Set.empty (Codec.schema codec) writerSchema

    let
        decoder =
            Bytes.makeDecoder Bytes.emptyEnvironment readSchema

    pure $ do
        values <-
            decoder

        case Codec.decoder codec values of
            Nothing ->
                fail "Couldn't extract typed value from parsed avro"
            Just a ->
                pure a




{-| Make a binary encoder for data using an Avro Codec
-}
makeEncoder :: Codec.Codec a -> a -> Put
makeEncoder codec =
    Bytes.encodeValue . Codec.writer codec

