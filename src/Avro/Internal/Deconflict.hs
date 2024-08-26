{-# LANGUAGE TupleSections #-}
module Avro.Internal.Deconflict (deconflict) where

import           Avro.Name (TypeName)
import qualified Avro.Name as Name
import           Avro.Schema (Schema(..), SchemaMismatch(..), typeName, Field (..))
import           Control.Arrow (left)
import           Control.Monad (foldM)
import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import           Avro.Internal.ReadSchema (ReadSchema)
import qualified Avro.Internal.ReadSchema as ReadSchema


nameAndAliasesFor :: Schema -> Maybe (TypeName, [TypeName])
nameAndAliasesFor reader =
    case reader of
        Enum name aliases _ _ _ ->
            Just (name, aliases)

        Record name aliases _ _  ->
            Just (name, aliases)

        Fixed name aliases _ _  ->
            Just (name, aliases)

        _ ->
            Nothing


{-| A function to deconflict a reader and writer Schema

This allows values to be read by a different schema from
whence it was written.

-}
deconflict :: Map TypeName TypeName -> Schema -> Schema -> Either SchemaMismatch ReadSchema
deconflict environmentNames readSchema writerSchema =
    let
        basicError =
            Left $
                TypeMismatch readSchema writerSchema
    in
    case readSchema of
        Null ->
            case writerSchema of
                Null ->
                    Right ReadSchema.Null

                _ ->
                    basicError

        Boolean ->
            case writerSchema of
                Boolean ->
                    Right ReadSchema.Boolean

                _ ->
                    basicError


        Int _ ->
            case writerSchema of
                Int _ ->
                    Right ReadSchema.Int

                _ ->
                    basicError

        Long _ ->
            case writerSchema of
                Int _ ->
                    Right ReadSchema.IntAsLong

                Long _ ->
                    Right ReadSchema.Long

                _ ->
                    basicError

        Float ->
            case writerSchema of
                Int _ ->
                    Right ReadSchema.IntAsFloat

                Long _ ->
                    Right ReadSchema.LongAsFloat

                Float ->
                    Right ReadSchema.Float

                _ ->
                    basicError

        Double ->
            case writerSchema of
                Int _ ->
                    Right ReadSchema.IntAsDouble

                Long _ ->
                    Right ReadSchema.LongAsDouble

                Float ->
                    Right ReadSchema.FloatAsDouble

                Double ->
                    Right ReadSchema.Double

                _ ->
                    basicError

        Bytes _ ->
            case writerSchema of
                Bytes _ ->
                    Right ReadSchema.Bytes

                String _ ->
                    Right ReadSchema.Bytes

                _ ->
                    basicError

        String _ ->
            case writerSchema of
                Bytes _ ->
                    Right ReadSchema.String

                String _ ->
                    Right ReadSchema.String

                _ ->
                    basicError


        Array readElem ->
            case writerSchema of
                Array writeElem ->
                    ReadSchema.Array <$>
                        deconflict environmentNames readElem writeElem

                _ ->
                    basicError

        Map readElem ->
            case writerSchema of
                Map writeElem ->
                    ReadSchema.Map <$>
                        deconflict environmentNames readElem writeElem

                _ ->
                    basicError


        Record readerName readerAliases _ readerFields ->
            case writerSchema of
                Record _ _ _ writerFields  ->
                    let
                        nestedEnvironment =
                            Map.union environmentNames
                                $ Map.fromList
                                $ map (,readerName) (readerName : readerAliases)

                        matching w ( r, _ ) =
                            fieldName r
                                == fieldName w
                                || List.elem (fieldName w) (fieldAliases r)

                        step work (written, remains) =
                            case work of
                                [] ->
                                    let
                                        maybeDefaults =
                                            foldM
                                                (\ known ( unwritten, ix ) ->
                                                    case fieldDefault unwritten of
                                                        Just d ->
                                                            Right (Map.insert ix d known)

                                                        Nothing ->
                                                            Left (MissingField readerName (fieldName unwritten))
                                                )
                                                Map.empty
                                                remains
                                    in
                                    ReadSchema.Record
                                        readerName
                                        (List.reverse written)
                                        <$> maybeDefaults


                                w : ws ->
                                    case pick (matching w) remains of
                                        Just ( ( r, ix ), more ) ->
                                            left (FieldMismatch readerName (fieldName w)) $ do
                                                dr <- deconflict nestedEnvironment (fieldType r) (fieldType w)
                                                let
                                                    readField =
                                                        ReadSchema.ReadField (fieldName r) dr (Just ix)
                                                step ws (readField : written, more)

                                        Nothing -> do
                                            dr <- deconflict nestedEnvironment (fieldType w) (fieldType w)
                                            let
                                                readField =
                                                    ReadSchema.ReadField (fieldName w) dr Nothing
                                            step ws (readField : written, remains)

                    in
                    step writerFields ([], List.zip readerFields [0 ..])

                _ ->
                    basicError


        Union readInfo ->
            let
                --
                -- Three pass search algorithm.
                --
                -- a) search by the full names of the elements;
                -- b) if nothing is found, search by the base names of the elements,
                --    or the full names of the aliases;
                -- c) if nothing is found, search by whether they can be deconflicted.
                --
                -- we do not backtrack if there are errors deconflicting from the
                -- initial searches.
                resolveBranch branchWriter continuation =
                    case findWithIndex (exactlyNamed (typeName branchWriter)) readInfo of
                        Just ( r, ix ) ->
                            deconflict environmentNames r branchWriter
                                >>= continuation ix

                        Nothing ->
                            case findWithIndex (compatiblyNamed (typeName branchWriter)) readInfo of
                                Just ( r, ix ) ->
                                    deconflict environmentNames r branchWriter
                                        >>= continuation ix

                                Nothing ->
                                    case findOk (\r -> deconflict environmentNames r branchWriter) readInfo of
                                        Just ( r, ix ) ->
                                            continuation ix r

                                        Nothing ->
                                            Left $ MissingUnion (typeName branchWriter)

                exactlyNamed writerInfo reader =
                    typeName reader == writerInfo

                compatiblyNamed writerInfo reader =
                    (`Name.compatibleNames` writerInfo) $
                        Maybe.fromMaybe (typeName reader, []) $
                            nameAndAliasesFor reader

            in
            case writerSchema of
                Union writerInfo ->
                    let
                        step work acc =
                            case work of
                                [] ->
                                    Right $
                                        ReadSchema.Union (List.reverse acc)

                                w : ws ->
                                    resolveBranch w (\ ix dr -> step ws (( ix, dr ) : acc ))
                    in
                    step writerInfo []

                singlular ->
                    resolveBranch singlular (\ix a -> Right (ReadSchema.AsUnion ix a))



        Enum readerName readerAliases _ readerSymbols readerDefault ->
            case writerSchema of
                Enum writerName _ _  writerSymbols _ | Name.compatibleNames (readerName, readerAliases) writerName ->
                    let
                        match writeSymbol =
                            case List.elemIndex writeSymbol readerSymbols of
                                Just ix ->
                                    Right ix

                                Nothing ->
                                    case readerDefault of
                                        Just def ->
                                            case List.elemIndex def readerSymbols of
                                                Just ix ->
                                                    Right ix

                                                Nothing ->
                                                    Left $ MissingSymbol def

                                        Nothing ->
                                            Left $ MissingSymbol writeSymbol

                        lined =
                            traverse match writerSymbols
                    in
                    ReadSchema.Enum readerName
                        <$> lined

                _ ->
                    basicError


        Fixed readerName readerAliases readerSize _ ->
            case writerSchema of
                Fixed writerName _ writerSize _  | Name.compatibleNames (readerName, readerAliases) writerName ->
                    if readerSize == writerSize then
                        Right $
                            ReadSchema.Fixed
                                readerName
                                readerSize

                    else
                        Left $ FixedWrongSize readerName readerSize writerSize

                _ ->
                    basicError


        NamedType readerName ->
            case writerSchema of
                NamedType writerName ->
                    case Map.lookup writerName environmentNames of
                        Just n ->
                            if n == readerName then
                                Right (ReadSchema.NamedType readerName)

                            else
                                basicError

                        Nothing ->
                            Left (NamedTypeUnresolved writerName)

                _ ->
                    Left (NamedTypeUnresolved readerName)



pick :: (a -> Bool) ->  [a] -> Maybe ( a, [a] )
pick f =
    let
        go seen input =
            case input of
                x : xs ->
                    if f x then
                        Just ( x, List.reverse seen ++ xs )

                    else
                        go (x : seen) xs

                _ ->
                    Nothing
    in
    go []



findWithIndex :: (a -> Bool) -> [a] -> Maybe ( a, Int )
findWithIndex f =
    let
        go i input =
            case input of
                x : xs ->
                    if f x then
                        Just ( x, i )

                    else
                        go (i + 1) xs

                _ ->
                    Nothing
    in
    go 0


findOk :: (a -> Either e b) -> [a] -> Maybe ( b, Int )
findOk f =
    let
        go i input =
            case input of
                x : xs ->
                    case f x of
                        Right b ->
                            Just ( b, i )

                        Left _ ->
                            go (i + 1) xs

                _ ->
                    Nothing
    in
    go 0
