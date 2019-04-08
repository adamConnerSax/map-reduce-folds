{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE OverloadedLists       #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.GroupBy
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental
-}
module Control.MapReduce.Engines.GroupBy
  ( -- * General groupBy in the form we want
    groupBy
    -- * Specific to keys with Ord k or (Hashable k, Eq k)
  , groupByOrderedKey
  , groupByHashableKey
  )
where

import           Data.Bool                      ( bool )
import qualified Data.DList                    as DL
import           Data.DList                     ( DList )
import           Data.Function                  ( on )
import qualified Data.Map.Strict               as MS
import qualified Data.HashMap.Strict           as HMS
import           Data.Maybe                     ( fromMaybe )
import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL
import           GHC.Exts                       ( IsList
                                                , Item
                                                )

{-|
General groupBy capturing the idea that we fold to some grouping structure and then fold that structure back to our
original container.
-}
-- TODO: Is it beneficial to replace the pair ((t -> [(k,l)]),(forall a. FL.Fold a (g a))) with (t -> g (k,l)) ?
groupBy
  :: forall t k c v l g
   . (Foldable g, Functor g)
  => FL.Fold (k, v) t -- ^ fold to tree
  -> (t -> [(k, l)]) -- ^ tree to List
  -> (forall a . FL.Fold a (g a)) -- ^ fold to g
  -> g (k, v)
  -> g (k, l)
groupBy foldToMap mapToList foldOut x =
  FL.fold foldOut . mapToList . FL.fold foldToMap $ x
{-# INLINABLE groupBy #-}

groupByOrderedKey
  :: forall g k v l
   . (Ord k, Semigroup l, Foldable g, Functor g)
  => (v -> l)
  -> (forall a . FL.Fold a (g a))
  -> g (k, v)
  -> g (k, l)
groupByOrderedKey promote = groupBy foldToStrictMap MS.toList
 where
  foldToStrictMap = FL.premap (second promote)
    $ FL.Fold (\t (k, l) -> MS.insertWith (\x y -> x <> y) k l t) MS.empty id
{-# INLINABLE groupByOrderedKey #-}

groupByHashableKey
  :: forall g k v l
   . (Hashable k, Eq k, Semigroup l, Foldable g, Functor g)
  => (v -> l)
  -> (forall a . FL.Fold a (g a))
  -> g (k, v)
  -> g (k, l)
groupByHashableKey promote = groupBy foldToStrictHashMap HMS.toList
 where
  foldToStrictHashMap = FL.premap (second promote)
    $ FL.Fold (\t (k, l) -> HMS.insertWith (<>) k l t) HMS.empty id
{-# INLINABLE groupByHashableKey #-}


