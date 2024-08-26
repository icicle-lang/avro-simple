{-# LANGUAGE RecordWildCards #-}
module Avro
    ( makeDecoder, makeEncoder

    , Environment (..), makeDecoderInEnvironment
    ) where

import qualified Avro.Codec as Codec
import qualified Avro.Internal.Bytes as Bytes
import           Avro.Internal.Deconflict (deconflict)
import           Avro.Schema (Schema, SchemaMismatch)
import           Data.Binary (Get, Put)
import qualified Data.Map as Map
import qualified Avro.Internal.Overlay as Overlay

{-| Create a binary decoder for avro data given a Codec and the writer's Schema.

Fields in Avro data types are not tagged, and records can only be interpreted
knowing the exact Schema with which they were written.

Therefore, building a binary decoder not requires a Codec for the type of
interest, but also the writer of the data's [`Schema`](Avro-Schema#Schema).

-}
makeDecoder :: Codec.Codec a -> Schema -> Either SchemaMismatch (Get a)
makeDecoder =
    makeDecoderInEnvironment (Environment [] [])

data Environment =
    Environment {
        readerEnvironment :: [Schema],
        writerEnvironment :: [Schema]
    }


makeDecoderInEnvironment :: Environment -> Codec.Codec a -> Schema -> Either SchemaMismatch (Get a)
makeDecoderInEnvironment env Codec.Codec {..} writerSchema = do
    let
        overlayedCodec =
            Codec.Codec (Overlay.overlays schema (readerEnvironment env))
                decoder writer

        overlayedWriter =
            Overlay.overlays writerSchema (writerEnvironment env)

    readSchema <-
        deconflict Map.empty (Codec.schema overlayedCodec) overlayedWriter

    pure $ do
        values <-
            Bytes.makeDecoder Bytes.emptyEnvironment readSchema

        case Codec.decoder overlayedCodec values of
            Nothing ->
                fail "Couldn't extract typed value from parsed avro"
            Just a ->
                pure a



{-| Make a binary encoder for data using an Avro Codec
-}
makeEncoder :: Codec.Codec a -> a -> Put
makeEncoder codec =
    Bytes.encodeValue . Codec.writer codec

