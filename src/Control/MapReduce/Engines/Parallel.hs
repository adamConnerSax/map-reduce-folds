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
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.Parallel
Description : Gatherer code for map-reduce-folds
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Some basic attempts at parallel map-reduce folds with lists as intermediate type.  There are multiple places we can do things in parallel.

(1) We can map (unpack/assign) in parallel.
(2) We can reduce in parallel.
(3) We can fold our intermediate monoid (in the gatherer) in parallel
(4) We can fold our result monoid in parallel

So far these are sometimes faster and sometimes slower than their serial counterparts.  So there is much room
for improvement, I think.
-}
module Control.MapReduce.Engines.Parallel
  (
    -- * lazy hash map grouping parallel list map-reduce 
    parallelMapReduceFold
    -- * A basic parallel mapReduceFold builder
  , parallelListEngine
    -- * parallel monoid folds
  , parFoldMonoid
  , parFoldMonoidDC
  -- * re-exports
  , NFData
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE
import qualified Control.MapReduce.Engines.List
                                               as MRL

import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.List                     as L
import qualified Data.List.Split               as L
import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashMap.Lazy             as HML
import           Data.Monoid                    ( Monoid
                                                , mconcat
                                                )

import qualified Control.Parallel.Strategies   as PS
import           Control.Parallel.Strategies    ( NFData ) -- for re-export
--


parallelMapReduceFold
  :: (NFData k, NFData c, NFData d, Hashable k, Eq k)
  => Int
  -> MRE.MapReduceFold y k c [] x d
parallelMapReduceFold numThreads = parallelListEngine
  numThreads
  (HMS.toList . HMS.fromListWith (<>) . fmap (second (pure @[])))

-- | Parallel map-reduce-fold list engine.  Uses the given parameters to use multiple sparks when mapping and reducing.
-- Chunks the input to numThreads chunks and sparks each chunk for mapping, merges the results, groups, then uses the same chunking and merging to do the reductions.
-- grouping could also be parallel but that is under the control of the given function.
parallelListEngine
  :: forall y k c x d
   . (NFData k, NFData c, NFData d)
  => Int
  -> ([(k, c)] -> [(k, [c])])
  -> MRE.MapReduceFold y k c [] x d
parallelListEngine numThreads groupByKey u (MRC.Assign a) r =
  let chunkedF :: FL.Fold x [[x]] =
        fmap (L.divvy numThreads numThreads) FL.list
      mappedF :: FL.Fold x [(k, c)] =
        fmap (L.concat . parMapEach (fmap a . MRL.unpackList u)) chunkedF
      groupedF :: FL.Fold x [(k, [c])] = fmap groupByKey mappedF
      reducedF :: FL.Fold x [d]        = fmap
        ( L.concat
        . parMapEach (fmap (uncurry $ MRE.reduceFunction r))
        . L.divvy numThreads numThreads
        )
        groupedF
  in  reducedF

parMapEach :: PS.NFData b => (a -> b) -> [a] -> [b]
parMapEach = PS.parMap (PS.rparWith PS.rdeepseq)
{-# INLINABLE parMapEach #-}

parMapChunk :: PS.NFData b => Int -> (a -> b) -> [a] -> [b]
parMapChunk chunkSize f =
  PS.withStrategy (PS.parListChunk chunkSize (PS.rparWith PS.rdeepseq)) . fmap f
{-# INLINABLE parMapChunk #-}

-- | like `foldMap id` but does each chunk of chunkSize in || until list is shorter than chunkSize
parFoldMonoid
  :: forall f m . (Foldable f, Monoid m, PS.NFData m) => Int -> f m -> m
parFoldMonoid chunkSize fm = go (F.toList fm)
 where
  go lm = case L.length lm > chunkSize of
    True ->
      go (PS.parMap (PS.rparWith PS.rdeepseq) F.fold $ L.chunksOf chunkSize lm)
    False -> F.fold lm
{-# INLINABLE parFoldMonoid #-}

-- | like `foldMap id` but does sublists in || first
parFoldMonoid'
  :: forall f m . (Foldable f, Monoid m, PS.NFData m) => Int -> f m -> m
parFoldMonoid' threadsToUse fm =
  let asList  = F.toList fm
      chunked = L.divvy threadsToUse threadsToUse asList
  in  mconcat $ parMapEach mconcat chunked


parFoldMonoidDC :: (Monoid b, PS.NFData b, Foldable h) => Int -> h b -> b
parFoldMonoidDC chunkSize = parFold chunkSize FL.mconcat

-- | do a Control.Foldl fold in Parallel using a divide and conquer strategy
-- we pay (?) to convert to a list, though this may be fused away.
-- We divide the list in half until the chunks are below chunkSize, then we fold and use mappend to build back up
parFold :: (Monoid b, Foldable h) => Int -> FL.Fold a b -> h a -> b
parFold chunkSize fl ha = divConq
  (FL.fold fl)
  (FL.fold FL.list ha)
  (\x -> L.length x < chunkSize)
  (<>)
  (\l -> Just $ splitAt (L.length l `div` 2) l)

-- | This divide and conquer is from <https://simonmar.github.io/bib/papers/strategies.pdf>
divConq
  :: (a -> b) -- compute the result
  -> a -- the value
  -> (a -> Bool) -- par threshold reached?
  -> (b -> b -> b) -- combine results
  -> (a -> Maybe (a, a)) -- divide
  -> b
divConq f arg threshold conquer divide = go arg
 where
  go arg = case divide arg of
    Nothing       -> f arg
    Just (l0, r0) -> conquer l1 r1 `PS.using` strat
     where
      l1 = go l0
      r1 = go r0
      strat x = do
        r l1
        r r1
        return x
       where
        r | threshold arg = PS.rseq
          | otherwise     = PS.rpar



