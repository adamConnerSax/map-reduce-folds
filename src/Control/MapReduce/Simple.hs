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
  , reduceMMapWithKey
  -- * simplified map-reduce folds
  -- ** serial
  , simpleMapReduceFold
  , simpleMapReduceFoldM
  , basicListFold
  , basicListFoldM
  , unpackOnlyFold
  , unpackOnlyFoldM
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

noUnpack :: MR.Unpack Identity x x
noUnpack = MR.Unpack Identity
{-# INLINABLE noUnpack #-}

simpleUnpack :: (x -> y) -> MR.Unpack Identity x y
simpleUnpack f = MR.Unpack $ Identity . f
{-# INLINABLE simpleUnpack #-}

filterUnpack :: (x -> Bool) -> MR.Unpack Maybe x x
filterUnpack t = MR.Unpack $ \x -> if t x then Just x else Nothing
{-# INLINABLE filterUnpack #-}

assign :: forall k y c . (y -> k) -> (y -> c) -> MR.Assign k y c
assign getKey getCols = MR.Assign (\y -> (getKey y, getCols y))
{-# INLINABLE assign #-}

reduceMapWithKey :: (k -> y -> z) -> MR.Reduce k h x y -> MR.Reduce k h x z
reduceMapWithKey f r = case r of
  MR.Reduce     g  -> MR.Reduce $ \k hx -> f k (g k hx)
  MR.ReduceFold gf -> MR.ReduceFold $ \k -> fmap (f k) (gf k)
{-# INLINABLE reduceMapWithKey #-}

reduceMMapWithKey
  :: (k -> y -> z) -> MR.ReduceM m k h x y -> MR.ReduceM m k h x z
reduceMMapWithKey f r = case r of
  MR.ReduceM     g  -> MR.ReduceM $ \k hx -> fmap (f k) (g k hx)
  MR.ReduceFoldM gf -> MR.ReduceFoldM $ \k -> fmap (f k) (gf k)
{-# INLINABLE reduceMMapWithKey #-}

-- | The most common case is that the reduction doesn't depend on the key
-- So we add support functions for processing the data and then relabeling with the key
-- And we do this for the four variations of Reduce
processAndRelabel :: (h x -> y) -> (k -> y -> z) -> MR.Reduce k h x z
processAndRelabel process relabel = MR.Reduce $ \k hx -> relabel k (process hx)
{-# INLINABLE processAndRelabel #-}

processAndRelabelM
  :: Monad m => (h x -> m y) -> (k -> y -> z) -> MR.ReduceM m k h x z
processAndRelabelM processM relabel =
  MR.ReduceM $ \k hx -> fmap (relabel k) (processM hx)
{-# INLINABLE processAndRelabelM #-}

foldAndRelabel
  :: Foldable h => FL.Fold x y -> (k -> y -> z) -> MR.Reduce k h x z
foldAndRelabel fld relabel = MR.ReduceFold $ \k -> fmap (relabel k) fld
{-# INLINABLE foldAndRelabel #-}

foldAndRelabelM
  :: (Monad m, Foldable h)
  => FL.FoldM m x y
  -> (k -> y -> z)
  -> MR.ReduceM m k h x z
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
simpleMapReduceFold
  :: (Monoid e, ec e, Foldable h, Monoid gt, Traversable g)
  => MR.Gatherer ec gt k c (h c)
  -> MR.Unpack g x y
  -> MR.Assign k y c
  -> MR.Reduce k h c e
  -> FL.Fold x e
simpleMapReduceFold gatherer unpacker assigner reducer =
  MR.mapReduceFold MR.uagMapAllGatherEachFold gatherer unpacker assigner reducer
{-# INLINABLE simpleMapReduceFold #-}

simpleMapReduceFoldM
  :: (Monad m, Monoid e, ec e, Foldable h, Monoid gt, Traversable g)
  => MR.Gatherer ec gt k c (h c)
  -> MR.UnpackM m g x y
  -> MR.AssignM m k y c
  -> MR.ReduceM m k h c e
  -> FL.FoldM m x e
simpleMapReduceFoldM gatherer unpacker assigner reducer = MR.mapReduceFoldM
  MR.uagMapAllGatherEachFoldM
  gatherer
  unpacker
  assigner
  reducer
{-# INLINABLE simpleMapReduceFoldM #-}

basicListFold
  :: forall kc k y c x e g
   . (DefaultGatherer kc k c [c], Monoid e, Traversable g, kc k)
  => MR.Unpack g x y
  -> MR.Assign k y c
  -> MR.Reduce k [] c e
  -> FL.Fold x e
basicListFold = simpleMapReduceFold (defaultGatherer @kc (pure @[]))
{-# INLINABLE basicListFold #-}

basicListFoldM
  :: forall kc k m y c x e g
   . (Monad m, DefaultGatherer kc k c [c], Monoid e, Traversable g, kc k)
  => MR.UnpackM m g x y
  -> MR.AssignM m k y c
  -> MR.ReduceM m k [] c e
  -> FL.FoldM m x e
basicListFoldM = simpleMapReduceFoldM (defaultGatherer @kc (pure @[]))
{-# INLINABLE basicListFoldM #-}


unpackOnlyFold
  :: forall h g x y
   . (Applicative h, Monoid (h y), Traversable g, Foldable h)
  => MR.Unpack g x y
  -> FL.Fold x (h y)
unpackOnlyFold unpack = simpleMapReduceFold (MR.defaultOrdGatherer (pure @h))
                                            unpack
                                            (MR.Assign $ \y -> ((), y))
                                            (MR.Reduce $ const id) -- should this be a fold?
{-# INLINABLE unpackOnlyFold #-}

unpackOnlyFoldM
  :: forall h m g x y
   . (Monad m, Applicative h, Monoid (h y), Traversable g, Foldable h)
  => MR.UnpackM m g x y
  -> FL.FoldM m x (h y)
unpackOnlyFoldM unpack = simpleMapReduceFoldM
  (MR.defaultOrdGatherer (pure @h))
  unpack
  (MR.AssignM $ \y -> return ((), y))
  (MR.ReduceM $ \_ -> return . id) -- should this be a fold?
{-# INLINABLE unpackOnlyFoldM #-}

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
  => Int -- items per spark for folding and mapping
  -> Int -- number of sparks for reducing
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

parBasicListOrdFold
  :: forall k g y c x e
   . ( Monoid e
     , MRP.NFData e -- for the parallel reduce     
     , MRP.NFData k -- for the parallel assign
     , MRP.NFData c -- for the parallel assign
     , Traversable g
     , Ord k
     )
  => Int -- items per spark for folding and mapping
  -> Int -- number of sparks for reducing
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
