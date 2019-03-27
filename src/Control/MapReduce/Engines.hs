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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Control.MapReduce.Engines
  (
    -- * Types
    MapReduceFold
  , MapReduceFoldM
  -- * serial map-reduce engines
  , lazyHashMapListEngine
  , lazyMapListEngine
  , lazyHashMapListEngineM
  , lazyMapListEngineM
  -- * serial map-reduce engine builders
  , listEngine
  , listEngineM
  -- * reduce helpers
  , reduceFunction
  , reduceFunctionM
  )
where

import qualified Control.MapReduce.Core        as MRC

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Lazy             as HML
import qualified Data.Map                      as ML

import           Control.Arrow                  ( second )

-- | case analysis of Reduce
reduceFunction
  :: MRC.Reduce k x d -> k -> (forall h . (Foldable h, Functor h) => h x -> d)
reduceFunction (MRC.Reduce     f) = f
reduceFunction (MRC.ReduceFold f) = \k -> FL.fold (f k)
{-# INLINABLE reduceFunction #-}

-- | case analysis of ReduceM
reduceFunctionM
  :: Monad m
  => MRC.ReduceM m k x d
  -> k
  -> (forall h . (Foldable h, Functor h) => h x -> m d)
reduceFunctionM (MRC.ReduceM     f) = f
reduceFunctionM (MRC.ReduceFoldM f) = \k -> FL.foldM (f k)
{-# INLINABLE reduceFunctionM #-}

-- | describe the signature of a map-reduce-fold-engine
type MapReduceFold g y k c q x d = MRC.Unpack g x y -> MRC.Assign k y c -> MRC.Reduce k c d -> FL.Fold x (q d)

-- | describe the signature of a monadic (effectful) map-reduce-fold-engine
type MapReduceFoldM m g y k c q x d = MRC.UnpackM m g x y -> MRC.AssignM m k y c -> MRC.ReduceM m k c d -> FL.FoldM m x (q d)

-- | map-reduce-fold engine using (Hashable k, Eq k) keys and returning a [] result
lazyHashMapListEngine
  :: (Functor g, Foldable g, Hashable k, Eq k) => MapReduceFold g y k c [] x d
lazyHashMapListEngine =
  listEngine (HML.toList . HML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyHashMapListEngine #-}

-- | map-reduce-fold engine using (Ord k) keys and returning a [] result
lazyMapListEngine
  :: (Functor g, Foldable g, Ord k) => MapReduceFold g y k c [] x d
lazyMapListEngine =
  listEngine (ML.toList . ML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyMapListEngine #-}

-- | effectful map-reduce-fold engine using (Hashable k, Eq k) keys and returning a [] result
lazyHashMapListEngineM
  :: (Monad m, Traversable g, Hashable k, Eq k)
  => MapReduceFoldM m g y k c [] x d
lazyHashMapListEngineM =
  listEngineM (HML.toList . HML.fromListWith (<>) . fmap (second (pure @[])))

-- | effectful map-reduce-fold engine using (Ord k) keys and returning a [] result
lazyMapListEngineM
  :: (Monad m, Traversable g, Ord k) => MapReduceFoldM m g y k c [] x d
lazyMapListEngineM =
  listEngineM (ML.toList . ML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyHashMapListEngineM #-}

-- | map-reduce-fold engine builder using (Hashable k, Eq k) keys and returning a [] result
listEngine
  :: (Functor g, Foldable g)
  => ([(k, c)] -> [(k, [c])])
  -> MapReduceFold g y k c [] x d
listEngine groupByKey (MRC.Unpack u) (MRC.Assign a) r = fmap
  (fmap (uncurry $ reduceFunction r) . groupByKey . L.concat . fmap
    (fmap a . F.toList . u)
  )
  FL.list
{-# INLINABLE listEngine #-}

-- | effectful map-reduce-fold engine builder using (Hashable k, Eq k) keys and returning a [] result
listEngineM
  :: (Monad m, Traversable g)
  => ([(k, c)] -> [(k, [c])])
  -> MapReduceFoldM m g y k c [] x d
listEngineM groupByKey (MRC.UnpackM u) (MRC.AssignM a) rM = MRC.postMapM
  ( join
  . fmap
      ( traverse (uncurry $ reduceFunctionM rM)
      . groupByKey
      . L.concat
      . fmap F.toList
      )
  . traverse ((traverse a =<<) . u)
  )
  (FL.generalize FL.list)
{-# INLINABLE listEngineM #-}

