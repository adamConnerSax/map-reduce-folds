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
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE BangPatterns          #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Simple
Description : Simplified interfaces and helper functions for map-reduce-folds
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Helper functions and default Engines and grouping functions for assembling map/reduce folds.
-}
module Control.MapReduce.Simple
  (
  -- * Unpackers
    noUnpack
  , simpleUnpack
  , filterUnpack

  -- * Assigners
  , assign

  -- * Reducers
  -- $reducers
  , processAndLabel
  , processAndLabelM
  , foldAndLabel
  , foldAndLabelM

  -- * Reduce Transformers 
  , reduceMapWithKey
  , reduceMMapWithKey

  -- * Default Map-Reduce Folds to @[]@
  , mapReduceFold
  , mapReduceFoldM
  , hashableMapReduceFold
  , hashableMapReduceFoldM
  , unpackOnlyFold
  , unpackOnlyFoldM

  -- * Simplify Results
  , concatFold
  , concatFoldM

  -- * Re-Exports
  , module Control.MapReduce.Core
  , Hashable
  )
where

import qualified Control.MapReduce.Core        as MR
import           Control.MapReduce.Core -- for re-export.  I don't like the unqualified-ness of this.
import qualified Control.MapReduce.Engines.Streaming
                                               as MRST
import qualified Control.MapReduce.Engines.Streamly
                                               as MRSL
import qualified Control.MapReduce.Engines.List
                                               as MRL

--import qualified Control.MapReduce.Parallel    as MRP

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(Identity)
                                                , runIdentity
                                                )

import           Data.Hashable                  ( Hashable )

-- | Don't do anything in the unpacking stage
noUnpack :: MR.Unpack x x
noUnpack = MR.Filter $ const True
{-# INLINABLE noUnpack #-}

-- | unpack using the given function
simpleUnpack :: (x -> y) -> MR.Unpack x y
simpleUnpack f = MR.Unpack $ Identity . f
{-# INLINABLE simpleUnpack #-}

-- | Filter while unpacking, using the given function
filterUnpack :: (x -> Bool) -> MR.Unpack x x
filterUnpack = MR.Filter
{-# INLINABLE filterUnpack #-}

-- | Assign via two functions of @y@, one that provides the key and one that provides the data to be grouped by that key.
assign :: forall k y c . (y -> k) -> (y -> c) -> MR.Assign k y c
assign getKey getCols = let f !y = (getKey y, getCols y) in MR.Assign f
{-# INLINABLE assign #-}

-- | map a reduce using the given function of key and reduction result.  
reduceMapWithKey :: (k -> y -> z) -> MR.Reduce k x y -> MR.Reduce k x z
reduceMapWithKey f r = case r of
  MR.Reduce     g  -> MR.Reduce $ \k -> fmap (f k) (g k)
  MR.ReduceFold gf -> MR.ReduceFold $ \k -> fmap (f k) (gf k)
{-# INLINABLE reduceMapWithKey #-}

-- | map a monadic reduction with a (non-monadic) function of the key and reduction result
reduceMMapWithKey :: (k -> y -> z) -> MR.ReduceM m k x y -> MR.ReduceM m k x z
reduceMMapWithKey f r = case r of
  MR.ReduceM     g  -> MR.ReduceM $ \k -> fmap (fmap (f k)) (g k)
  MR.ReduceFoldM gf -> MR.ReduceFoldM $ \k -> fmap (f k) (gf k)
{-# INLINABLE reduceMMapWithKey #-}


{- $reducers
The most common case is that the reduction doesn't depend on the key.
These functions combine a key-independent processing step and a labeling step for the four variations of @Reduce@.
-}

-- | create a Reduce from a function of the grouped data to y and a function from the key and y to the result type
processAndLabel
  :: (forall h . (Foldable h, Functor h) => h x -> y)
  -> (k -> y -> z)
  -> MR.Reduce k x z
processAndLabel process relabel = MR.Reduce $ \k -> relabel k . process
{-# INLINABLE processAndLabel #-}

-- | create a monadic ReduceM from a function of the grouped data to (m y) and a function from the key and y to the result type
processAndLabelM
  :: Monad m
  => (forall h . (Foldable h, Functor h) => h x -> m y)
  -> (k -> y -> z)
  -> MR.ReduceM m k x z
processAndLabelM processM relabel =
  MR.ReduceM $ \k -> fmap (relabel k) . processM
{-# INLINABLE processAndLabelM #-}

-- | create a Reduce from a fold of the grouped data to y and a function from the key and y to the result type
foldAndLabel :: FL.Fold x y -> (k -> y -> z) -> MR.Reduce k x z
foldAndLabel fld relabel = let q !k = fmap (relabel k) fld in MR.ReduceFold q
{-# INLINABLE foldAndLabel #-}

-- | create a monadic ReduceM from a monadic fold of the grouped data to (m y) and a function from the key and y to the result type
foldAndLabelM
  :: Monad m => FL.FoldM m x y -> (k -> y -> z) -> MR.ReduceM m k x z
foldAndLabelM fld relabel =
  let q !k = fmap (relabel k) fld in MR.ReduceFoldM q
{-# INLINABLE foldAndLabelM #-}

-- | The simple fold types return lists of results.  Often we want to merge these into some other structure via (<>)
concatFold :: (Monoid d, Foldable g) => FL.Fold a (g d) -> FL.Fold a d
concatFold = fmap F.fold

-- | The simple fold types return lists of results.  Often we want to merge these into some other structure via (<>)
concatFoldM
  :: (Monad m, Monoid d, Foldable g) => FL.FoldM m a (g d) -> FL.FoldM m a d
concatFoldM = fmap F.fold

mapReduceFold
  :: Ord k
  => MR.Unpack x y -- ^ unpack x to none or one or many y's
  -> MR.Assign k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.Reduce k c d -- ^ reduce a grouped [c] to d
  -> FL.Fold x [d]
mapReduceFold u a r =
  fmap (runIdentity . MRSL.resultToList)
    $ MRSL.streamlyEngine MRSL.groupByOrderedKey u a r
{-# INLINABLE mapReduceFold #-}

mapReduceFoldM
  :: (Monad m, Ord k)
  => MR.UnpackM m x y -- ^ unpack x to none or one or many y's
  -> MR.AssignM m k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.ReduceM m k c d -- ^ reduce a grouped [c] to d
  -> FL.FoldM m x [d]
mapReduceFoldM u a r =
  MR.postMapM id $ fmap MRST.resultToList $ MRST.streamingEngineM
    MRST.groupByOrderedKey
    u
    a
    r
{-# INLINABLE mapReduceFoldM #-}

hashableMapReduceFold
  :: (Hashable k, Eq k)
  => MR.Unpack x y -- ^ unpack x to none or one or many y's
  -> MR.Assign k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.Reduce k c d -- ^ reduce a grouped [c] to d
  -> FL.Fold x [d]
hashableMapReduceFold u a r =
  fmap (runIdentity . MRSL.resultToList)
    $ MRSL.streamlyEngine MRSL.groupByHashableKey u a r
{-# INLINABLE hashableMapReduceFold #-}

hashableMapReduceFoldM
  :: (Monad m, Hashable k, Eq k)
  => MR.UnpackM m x y -- ^ unpack x to to none or one or many y's
  -> MR.AssignM m k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.ReduceM m k c d -- ^ reduce a grouped [c] to d
  -> FL.FoldM m x [d]
hashableMapReduceFoldM u a r =
  MR.postMapM id $ fmap MRST.resultToList $ MRST.streamingEngineM
    MRST.groupByHashableKey
    u
    a
    r
{-# INLINABLE hashableMapReduceFoldM #-}

-- | do only the unpack step.
unpackOnlyFold :: MR.Unpack x y -> FL.Fold x [y]
unpackOnlyFold u = fmap (MRL.unpackList u) FL.list
{-# INLINABLE unpackOnlyFold #-}

-- | do only the (monadic) unpack step. Use a TypeApplication to specify what to unpack to. As in 'unpackOnlyFoldM @[]'
unpackOnlyFoldM :: Monad m => MR.UnpackM m x y -> FL.FoldM m x [y]
unpackOnlyFoldM u = MR.postMapM (MRL.unpackListM u) (FL.generalize FL.list)
{-# INLINABLE unpackOnlyFoldM #-}
