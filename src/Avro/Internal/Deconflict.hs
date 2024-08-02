module Avro.Internal.Deconflict where

import           Avro.Name (TypeName)
import qualified Avro.Name as Name
import           Avro.Schema (Schema(..), SchemaMismatch(..), typeName, Field (..))
import           Control.Monad (foldM)
import qualified Data.Map as Map
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Text (Text)
import           Avro.Internal.ReadSchema (ReadSchema)
import qualified Avro.Internal.ReadSchema as ReadSchema



canonicalNamesForSchema :: Schema -> [Text]
canonicalNamesForSchema schema =
    case nameAndAliasesFor schema of
        Just (name, aliases) ->
            Name.baseName . Name.canonicalName
                <$> name : aliases

        Nothing ->
            []


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
deconflict :: Set Text -> Schema -> Schema -> Either SchemaMismatch ReadSchema
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


        Record readerName _ _ readerFields ->
            case writerSchema of
                Record _ _ _ writerFields  ->
                    let
                        nestedEnvironment =
                            Set.union environmentNames (Set.fromList (canonicalNamesForSchema readSchema))

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
                                        Just ( ( r, ix ), more ) -> do
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
                -- Three pass search algorithm:
                --   first we search by the full names of the elements;
                --   then we search by the base names of the elements;
                --   then we search by whether they can be deconflicted.
                resolveBranch branchWriter continuation =
                    case findWithIndex (identicalNames (typeName branchWriter)) readInfo of
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

                identicalNames writerInfo reader =
                    ((==) writerInfo . fst) $
                        Maybe.fromMaybe (typeName reader, []) $
                            nameAndAliasesFor reader

                compatiblyNamed writeInfo reader =
                    (`Name.compatibleNames` writeInfo) $
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



        Enum readerName _ _ readerSymbols readerDefault ->
            case writerSchema of
                Enum _ _ _  writerSymbols _ ->
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


        Fixed readerName _ readerSize _ ->
            case writerSchema of
                Fixed _ _ writerSize _ ->
                    if readerSize == writerSize then
                        Right $
                            ReadSchema.Fixed
                                readerName
                                readerSize

                    else
                        basicError

                _ ->
                    basicError


        NamedType readerName ->
            case writerSchema of
                NamedType writerName ->
                    if readerName == writerName then
                        if Set.member (Name.baseName (Name.canonicalName writerName)) environmentNames then
                            Right (ReadSchema.NamedType readerName)

                        else
                            Left (NamedTypeUnresolved writerName)

                    else
                        basicError

                _ ->
                    basicError



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
