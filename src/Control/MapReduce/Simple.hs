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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
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
  -- * simplified map-reduce folds
  -- ** serial
  , mapReduceFold
  , basicListFold
  , unpackOnlyFold
  -- ** parallel (non-mondaic folds only)
  , parBasicListHashableFold
  , parBasicListOrdFold
  )
where

import qualified Control.MapReduce.Core        as MR
import qualified Control.MapReduce.Gatherer    as MR
import qualified Control.MapReduce.Parallel    as MRP

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity(Identity) )
import           Data.Monoid                    ( Monoid(..) )
import qualified Data.Sequence                 as Seq

import           Data.Hashable                  ( Hashable )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )

noUnpack :: MR.Unpack 'Nothing Identity x x
noUnpack = MR.Unpack Identity
{-# INLINABLE noUnpack #-}

simpleUnpack :: (x -> y) -> MR.Unpack 'Nothing Identity x y
simpleUnpack f = MR.Unpack $ Identity . f
{-# INLINABLE simpleUnpack #-}

filterUnpack :: (x -> Bool) -> MR.Unpack 'Nothing Maybe x x
filterUnpack t = MR.Unpack $ \x -> if t x then Just x else Nothing
{-# INLINABLE filterUnpack #-}

assign :: forall k y c . (y -> k) -> (y -> c) -> MR.Assign 'Nothing k y c
assign getKey getCols = MR.Assign (\y -> (getKey y, getCols y))
{-# INLINABLE assign #-}

reduceMapWithKey
  :: (k -> y -> z) -> MR.Reduce mm k h x y -> MR.Reduce mm k h x z
reduceMapWithKey f r = case r of
  MR.Reduce      g  -> MR.Reduce $ \k hx -> f k (g k hx)
  MR.ReduceFold  gf -> MR.ReduceFold $ \k -> fmap (f k) (gf k)
  MR.ReduceM     g  -> MR.ReduceM $ \k hx -> fmap (f k) (g k hx)
  MR.ReduceFoldM gf -> MR.ReduceFoldM $ \k -> fmap (f k) (gf k)
{-# INLINABLE reduceMapWithKey #-}

-- | The most common case is that the reduction doesn't depend on the key
-- So we add support functions for processing the data and then relabeling with the key
-- And we do this for the four variations of Reduce
processAndRelabel :: (h x -> y) -> (k -> y -> z) -> MR.Reduce 'Nothing k h x z
processAndRelabel process relabel = MR.Reduce $ \k hx -> relabel k (process hx)
{-# INLINABLE processAndRelabel #-}

processAndRelabelM
  :: Monad m => (h x -> m y) -> (k -> y -> z) -> MR.Reduce ( 'Just m) k h x z
processAndRelabelM processM relabel =
  MR.ReduceM $ \k hx -> fmap (relabel k) (processM hx)
{-# INLINABLE processAndRelabelM #-}

foldAndRelabel
  :: Foldable h => FL.Fold x y -> (k -> y -> z) -> MR.Reduce 'Nothing k h x z
foldAndRelabel fld relabel = MR.ReduceFold $ \k -> fmap (relabel k) fld
{-# INLINABLE foldAndRelabel #-}

foldAndRelabelM
  :: (Monad m, Foldable h)
  => FL.FoldM m x y
  -> (k -> y -> z)
  -> MR.Reduce ( 'Just m) k h x z
foldAndRelabelM fld relabel = MR.ReduceFoldM $ \k -> fmap (relabel k) fld
{-# INLINABLE foldAndRelabelM #-}

class DefaultGatherer (ce :: Type -> Constraint) k y d where
  defaultGatherer :: (ce k, Semigroup d) => (y -> d) -> MR.Gatherer MR.Empty (Seq.Seq (k, y)) k y d

instance Ord k => DefaultGatherer Ord k y d where
  defaultGatherer = MR.defaultOrdGatherer

instance (Hashable k, Eq k, Semigroup d) => DefaultGatherer Hashable k y d where
  defaultGatherer = MR.defaultHashableGatherer

-- | Basic mapReduce fold.  Assumes monomorphic monadic parameter (all 'Nothing or all ('Just m) for some Monad m),
-- and fixed to mapAllGatherEach as mapping step because it is the most general, requiring only Traversable g. 
mapReduceFold
  :: ( Monoid e
     , ec e
     , Foldable h
     , Monoid gt
     , Functor (MR.MapFoldT mm x)
     , Traversable g
     , MR.UAGMapFolds mm
     , MR.MapReduceF mm
     )
  => MR.Gatherer ec gt k c (h c)
  -> MR.Unpack mm g x y
  -> MR.Assign mm k y c
  -> MR.Reduce mm k h c e
  -> MR.MapFoldT mm x e
mapReduceFold gatherer unpacker assigner reducer = MR.mapReduceF
  gatherer
  (MR.mapAllGatherEachF gatherer unpacker assigner)
  reducer
{-# INLINABLE mapReduceFold #-}

basicListFold
  :: forall kc k y c mm x e g
   . ( DefaultGatherer kc k c [c]
     , Monoid e
     , Functor (MR.MapFoldT mm x)
     , Traversable g
     , MR.UAGMapFolds mm
     , MR.MapReduceF mm
     , kc k
     )
  => MR.Unpack mm g x y
  -> MR.Assign mm k y c
  -> MR.Reduce mm k [] c e
  -> MR.MapFoldT mm x e
basicListFold = mapReduceFold (defaultGatherer @kc (pure @[]))
{-# INLINABLE basicListFold #-}

unpackOnlyFold
  :: forall h mm g x y
   . ( Applicative h
     , Monoid (h y)
     , Traversable g
     , Foldable h
     , Functor (MR.MapFoldT mm x)
     , MR.MapReduceF mm
     , MR.UAGMapFolds mm
     , MR.IdStep mm
     )
  => MR.Unpack mm g x y
  -> MR.MapFoldT mm x (h y)
unpackOnlyFold unpack = mapReduceFold (MR.defaultOrdGatherer (pure @h))
                                      unpack
                                      MR.idAssigner
                                      MR.idReducer
{-# INLINABLE unpackOnlyFold #-}

parBasicListHashableFold
  :: forall k g y c x e
   . ( Monoid e
     , MRP.NFData e -- for the parallel reduce     
     , MRP.NFData k -- for the parallel assign
     , MRP.NFData c -- for the parallel assign
     , Functor (MR.MapFoldT 'Nothing x)
     , Traversable g
     , Hashable k
     , Eq k
     )
  => Int -- items per spark for folding and mapping
  -> Int -- number of sparks for reducing
  -> MR.Unpack 'Nothing g x y
  -> MR.Assign 'Nothing k y c
  -> MR.Reduce 'Nothing k [] c e
  -> MR.MapFoldT 'Nothing x e
parBasicListHashableFold oneSparkMax numThreads u a r =
  let g                        = MRP.parReduceGathererHashableL (pure @[])
      (MR.MapGather _ mapStep) = MR.uagMapAllGatherEachFold g u a
  in  MRP.parallelMapReduceF oneSparkMax numThreads g mapStep r
{-# INLINABLE parBasicListHashableFold #-}

parBasicListOrdFold
  :: forall k g y c x e
   . ( Monoid e
     , MRP.NFData e -- for the parallel reduce     
     , MRP.NFData k -- for the parallel assign
     , MRP.NFData c -- for the parallel assign
     , Functor (MR.MapFoldT 'Nothing x)
     , Traversable g
     , Ord k
     )
  => Int -- items per spark for folding and mapping
  -> Int -- number of sparks for reducing
  -> MR.Unpack 'Nothing g x y
  -> MR.Assign 'Nothing k y c
  -> MR.Reduce 'Nothing k [] c e
  -> MR.MapFoldT 'Nothing x e
parBasicListOrdFold oneSparkMax numThreads u a r =
  let g                        = MRP.parReduceGathererOrd (pure @[])
      (MR.MapGather _ mapStep) = MR.uagMapAllGatherEachFold g u a
  in  MRP.parallelMapReduceF oneSparkMax numThreads g mapStep r
{-# INLINABLE parBasicListOrdFold #-}
