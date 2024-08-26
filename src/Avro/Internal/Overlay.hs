{-# LANGUAGE TupleSections #-}
module Avro.Internal.Overlay (overlays) where

import qualified Data.List as List
import           Data.Map (Map)
import qualified Data.Map as Map
import           Avro.Schema (Schema (..), Field (..))
import           Avro.Name (TypeName)


{-| Traverse a schema and build a map of all declared types.
-}
extractBindings :: Schema -> Map TypeName Schema
extractBindings s =
    let
        selfBindings names aliases =
            Map.fromList $
                (, s) <$>
                    names : aliases

    in
    case s of
        Record names aliases _ fields ->
            let
                deeper =
                    Map.unions $
                        extractBindings . fieldType <$> fields
            in
            Map.union (selfBindings names aliases) deeper

        Enum names aliases _ _ _ ->
            selfBindings names aliases

        Fixed names aliases _ _ ->
            selfBindings names aliases

        Union options ->
            Map.unions $
                extractBindings <$> options

        Array items ->
            extractBindings items

        Map values ->
            extractBindings values

        _ ->
            Map.empty


{-| Substitute named types into a Schema.

It is common when building Avro schemas traditionally to write individual
types separately and compose them into larger objects.

This function will rebuild a complete Schema from small components so that
it is ready to encode and decode data.

Schemas are also parsed, in a depth first manner, left to right, allowing
fields in a record to use types defined in earlier fields (for example).

-}
overlays :: Schema -> [Schema] -> Schema
overlays input supplements =
    let
        bindings =
            Map.unions $
                extractBindings <$> supplements

        go env s =
            case s of
                NamedType nm ->
                    case Map.lookup nm env of
                        Just x ->
                            ( env, x )

                        Nothing ->
                            ( env, s )

                Enum name aliases _ _ _ ->
                    ( adjust name aliases s env, s )

                Fixed name aliases _ _ ->
                    ( adjust name aliases s env, s )


                Record name aliases doc fields ->
                    let
                        newEnv =
                            List.foldl' (flip Map.delete) env (name : aliases)

                        goField e fld =
                            let
                                ( ns, nt ) =
                                    go e (fieldType fld)
                            in
                            ( ns, fld { fieldType = nt })

                        ( fin, newFields ) =
                            List.mapAccumL goField newEnv fields
                    in
                    ( adjust name aliases s fin, Record name aliases doc newFields )


                Union options ->
                    let
                        ( fin, newOptions ) =
                            List.mapAccumL go env options
                    in
                    ( fin, Union newOptions )


                Array items ->
                    let
                        ( fin, newItems ) =
                            go env items
                    in
                    ( fin, Array newItems )

                Map values ->
                    let
                        ( fin, newValues ) =
                            go env values
                    in
                    ( fin, Map newValues )

                basic ->
                    ( env, basic )

        ( _, output ) =
            go bindings input
    in
    output



adjust :: TypeName -> [TypeName] -> Schema -> Map TypeName Schema -> Map TypeName Schema
adjust name aliases self env =
    Map.union env $
        Map.fromList $
            (, self) <$> name : aliases
