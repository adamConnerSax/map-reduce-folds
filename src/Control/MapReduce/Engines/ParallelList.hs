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
Module      : Control.MapReduce.Engines.ParallelList
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

NB: This does not seem to be faster--and is often slower!--than the serial engine.  I leave it here as a starting point for improvement.
-}
module Control.MapReduce.Engines.ParallelList
  (
    -- * Parallel map/reduce fold builder
    parallelListEngine

    -- * re-exports
  , NFData
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE
import qualified Control.MapReduce.Engines.List
                                               as MRL

import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import qualified Data.List.Split               as L

import qualified Control.Parallel.Strategies   as PS
import           Control.Parallel.Strategies    ( NFData ) -- for re-export
--


{- | Parallel map-reduce-fold list engine.  Uses the given parameters to use multiple sparks when mapping and reducing.
 Chunks the input to numThreads chunks and sparks each chunk for mapping, merges the results, groups, then uses the same chunking and merging to do the reductions.
 grouping could also be parallel but that is under the control of the given function.
-}
parallelListEngine
  :: forall g y k c x d
   . (NFData k, NFData c, NFData d, Foldable g, Functor g)
  => Int
  -> ([(k, c)] -> [(k, g c)])
  -> MRE.MapReduceFold y k c [] x d
parallelListEngine numThreads groupByKey u (MRC.Assign a) r =
  let chunkedF :: FL.Fold x [[x]] =
        fmap (L.divvy numThreads numThreads) FL.list
      mappedF :: FL.Fold x [(k, c)] =
        fmap (L.concat . parMapEach (fmap a . MRL.unpackList u)) chunkedF
      groupedF :: FL.Fold x [(k, g c)] = fmap groupByKey mappedF
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
