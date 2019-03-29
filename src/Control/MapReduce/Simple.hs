{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
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
  , processAndRelabel
  , processAndRelabelM
  , foldAndRelabel
  , foldAndRelabelM
  -- * reduce transformers 
  , reduceMapWithKey
  , reduceMMapWithKey
  -- * simplified map-reduce folds
  -- ** serial
  , mapReduceFold
  , mapReduceFoldM
  , hashableMapReduceFold
  , hashableMapReduceFoldM
  , unpackOnlyFold
  , unpackOnlyFoldM
  -- * re-exports
  , module Control.MapReduce.Core
  , Hashable
  )
where

import qualified Control.MapReduce.Core        as MR
import           Control.MapReduce.Core -- for re-export.  I don't like the unqualified-ness of this.
import qualified Control.MapReduce.Engines.Streams
                                               as MRS
import qualified Control.MapReduce.Engines.List
                                               as MRL

--import qualified Control.MapReduce.Parallel    as MRP

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity(Identity) )

import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.List                     as L

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
filterUnpack t = MR.Filter t
{-# INLINABLE filterUnpack #-}

-- | Assign via two functions, one that provides the key and one that provides the data to be grouped by that key
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

-- | The most common case is that the reduction doesn't depend on the key
-- So we add support functions for processing the data and then relabeling with the key
-- And we do this for the four variations of Reduce
-- create a Reduce from a function of the grouped data to y and a function from the key and y to the result type
processAndRelabel
  :: (forall h . (Foldable h, Functor h) => h x -> y)
  -> (k -> y -> z)
  -> MR.Reduce k x z
processAndRelabel process relabel = MR.Reduce $ \k -> relabel k . process
{-# INLINABLE processAndRelabel #-}

-- | create a monadic ReduceM from a function of the grouped data to (m y) and a function from the key and y to the result type
processAndRelabelM
  :: Monad m
  => (forall h . (Foldable h, Functor h) => h x -> m y)
  -> (k -> y -> z)
  -> MR.ReduceM m k x z
processAndRelabelM processM relabel =
  MR.ReduceM $ \k -> fmap (relabel k) . processM
{-# INLINABLE processAndRelabelM #-}

-- | create a Reduce from a fold of the grouped data to y and a function from the key and y to the result type
foldAndRelabel :: FL.Fold x y -> (k -> y -> z) -> MR.Reduce k x z
foldAndRelabel fld relabel =
  let q !k = fmap (relabel k) fld in MR.ReduceFold q
{-# INLINABLE foldAndRelabel #-}

-- | create a monadic ReduceM from a monadic fold of the grouped data to (m y) and a function from the key and y to the result type
foldAndRelabelM
  :: Monad m => FL.FoldM m x y -> (k -> y -> z) -> MR.ReduceM m k x z
foldAndRelabelM fld relabel =
  let q !k = fmap (relabel k) fld in MR.ReduceFoldM q
{-# INLINABLE foldAndRelabelM #-}

mapReduceFold
  :: Ord k
  => MR.Unpack x y -- ^ unpack x to none or one or many y's
  -> MR.Assign k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.Reduce k c d -- ^ reduce a grouped [c] to d
  -> FL.Fold x [d]
mapReduceFold = MRS.streamEngine MRS.groupByOrdKey
{-# INLINABLE mapReduceFold #-}

mapReduceFoldM
  :: (Monad m, Ord k)
  => MR.UnpackM m x y -- ^ unpack x to none or one or many y's
  -> MR.AssignM m k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.ReduceM m k c d -- ^ reduce a grouped [c] to d
  -> FL.FoldM m x [d]
mapReduceFoldM = MRS.streamEngineM MRS.groupByOrdKey
{-# INLINABLE mapReduceFoldM #-}

hashableMapReduceFold
  :: (Hashable k, Eq k)
  => MR.Unpack x y -- ^ unpack x to none or one or many y's
  -> MR.Assign k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.Reduce k c d -- ^ reduce a grouped [c] to d
  -> FL.Fold x [d]
hashableMapReduceFold = MRS.streamEngine MRS.groupByHashedKey
{-# INLINABLE hashableMapReduceFold #-}

hashableMapReduceFoldM
  :: (Monad m, Hashable k, Eq k)
  => MR.UnpackM m x y -- ^ unpack x to to none or one or many y's
  -> MR.AssignM m k y c -- ^ assign each y to a key value pair (k,c)
  -> MR.ReduceM m k c d -- ^ reduce a grouped [c] to d
  -> FL.FoldM m x [d]
hashableMapReduceFoldM = MRS.streamEngineM MRS.groupByHashedKey
{-# INLINABLE hashableMapReduceFoldM #-}

-- | do only the unpack step.
unpackOnlyFold :: MR.Unpack x y -> FL.Fold x [y]
unpackOnlyFold u = fmap (MRL.unpackList u) FL.list
{-# INLINABLE unpackOnlyFold #-}

-- | do only the (monadic) unpack step. Use a TypeApplication to specify what to unpack to. As in 'unpackOnlyFoldM @[]'
unpackOnlyFoldM :: Monad m => MR.UnpackM m x y -> FL.FoldM m x [y]
unpackOnlyFoldM u = MR.postMapM (MRL.unpackListM u) (FL.generalize FL.list)
{-# INLINABLE unpackOnlyFoldM #-}
{-
-- | basic parallel mapReduce, assumes Hashable key.  Takes two arguments to specify how things should be grouped.
parBasicListHashableFold
  :: forall k g y c x e
   . ( Monoid e
     , MRP.NFData e -- for the parallel reduce     
     , MRP.NFData k -- for the parallel assign
     , MRP.NFData c -- for the parallel assign
     , Traversable g
     , Hashable k
     , Eq k
     )
  => Int -- ^ items per spark for folding and mapping
  -> Int -- ^ number of sparks for reducing
  -> MR.Unpack g x y
  -> MR.Assign k y c
  -> MR.Reduce k [] c e
  -> FL.Fold x e
parBasicListHashableFold oneSparkMax numThreads u a r =
  let g = MRP.parReduceGathererHashableL (pure @[])
  in  MRP.parallelMapReduceFold oneSparkMax
                                numThreads
                                MR.uagMapAllGatherEachFold
                                g
                                u
                                a
                                r
{-# INLINABLE parBasicListHashableFold #-}

-- | basic parallel mapReduce, assumes Ord key.  Takes two arguments to specify how things should be grouped.
parBasicListOrdFold
  :: forall k g y c x e
   . ( Monoid e
     , MRP.NFData e -- for the parallel reduce     
     , MRP.NFData k -- for the parallel assign
     , MRP.NFData c -- for the parallel assign
     , Traversable g
     , Ord k
     )
  => Int -- ^ items per spark for folding and mapping
  -> Int -- ^ number of sparks for reducing
  -> MR.Unpack g x y
  -> MR.Assign k y c
  -> MR.Reduce k [] c e
  -> FL.Fold x e
parBasicListOrdFold oneSparkMax numThreads u a r =
  let g = MRP.parReduceGathererOrd (pure @[])
  in  MRP.parallelMapReduceFold oneSparkMax
                                numThreads
                                MR.uagMapAllGatherEachFold
                                g
                                u
                                a
                                r
{-# INLINABLE parBasicListOrdFold #-}
-}
