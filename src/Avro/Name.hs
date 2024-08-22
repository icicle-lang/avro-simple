{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
module Avro.Name
    ( TypeName (..)
    , contextualTypeName, canonicalName
    , compatibleNames
    ) where

import qualified Data.List as List
import qualified Data.Maybe as Maybe
import qualified Data.Text as Text
import           Data.Text (Text)
import qualified Data.Char as Char
import Data.Function (on)

{-| An Avro Type Name

This constructor is exposed, but one should only build names which
are correctly scoped and have valid names.

If unsure, one should use [`contextualTypeName`](Avro-Name#contextualTypeName)
to parse the data instead.

-}
data TypeName =
  TypeName
    { baseName :: Text
    , nameSpace :: [Text]
    } deriving (Show)

instance Eq TypeName where
  (==) = (==) `on` (baseName . canonicalName)

instance Ord TypeName where
  compare = compare `on` (baseName . canonicalName)


{-| Normalise a TypeName.

This replaces short names with fullnames, using applicable namespaces
to do so, then eliminate namespace attributes, which are now redundant.

-}
canonicalName :: TypeName -> TypeName
canonicalName TypeName { baseName, nameSpace} =
    let
        built =
            Text.intercalate "." (nameSpace ++ [baseName])
    in
    TypeName { baseName = built, nameSpace = [] }



{-| Build a TypeName from a qualified string.
-}
parseFullName :: Text -> Either String TypeName
parseFullName input =
    case unsnoc (splitNameParts input) of
        Just ( rest, base ) ->
            TypeName
                <$> validNamePart base
                <*> traverse validNamePart rest

        Nothing ->
            Left "Type names must contain non-empty valid name parts."



{-| Build a TypeName from using the name and namespace fields within a context.

Rules for this are specified in [the avro specification](https://avro.apache.org/docs/1.11.1/specification/#names).

Arguments:

  - Optional context (parent name)
  - Name
  - Optional Namespace

-}
contextualTypeName :: Maybe TypeName -> Text -> Maybe Text -> Either String TypeName
contextualTypeName context input explicit =
    if Maybe.isNothing (Text.findIndex (== '.') input) then
        case explicit of
            Just ns ->
                TypeName
                    <$> validNamePart input
                    <*> traverse validNamePart (splitNameParts ns)

            Nothing ->
                TypeName
                    <$> validNamePart input
                    <*> Right (maybe [] nameSpace context)

    else
        parseFullName input


{-| Whether two names are compatible for Schema resolution.

This means that either the unqualified names match, or an alias matches
the fully qualified name.

-}
compatibleNames :: (TypeName, [TypeName]) -> TypeName -> Bool
compatibleNames (readerName, readerAliases) writerName =
    baseName readerName
        == baseName writerName
        || List.elem writerName readerAliases




unsnoc :: [b] -> Maybe ( [b], b )
unsnoc list =
    let
        step x z =
            Just $
                case z of
                    Nothing ->
                        ( [], x )

                    Just ( a, b ) ->
                        ( x : a, b )
    in
    List.foldr step Nothing list




validNamePart :: Text -> Either String Text
validNamePart s =
    case Text.unpack s of
        c : cs ->
            if Char.isAlpha c && List.all Char.isAlphaNum cs then
                Right s

            else
                Left "Type name is not alpha-numeric"

        _ ->
            Left "Type name is empty"



{-| Split a name (or namespace)

Unfortunately, the Avro specification itself has issues
around names.

The filtering of empty lists is used to fixup some of
these.

-}
splitNameParts :: Text.Text -> [ Text.Text ]
splitNameParts input =
    if Text.null input then
        []

    else
        List.filter (not . Text.null) $
            Text.splitOn "." input

