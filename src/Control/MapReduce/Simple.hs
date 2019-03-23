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
module Control.MapReduce.Simple where

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

filter :: (x -> Bool) -> MR.Unpack 'Nothing Maybe x x
filter t = MR.Unpack $ \x -> if t x then Just x else Nothing
{-# INLINABLE filter #-}

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

basicListF
  :: forall kc k y c mm x e g
   . ( DefaultGatherer kc k c [c]
     , Monoid e
     , Functor (MR.MapFoldT mm x)
     , Traversable g
     , kc k
     )
  => MR.Unpack mm g x y
  -> MR.Assign mm k y c
  -> MR.Reduce mm k [] c e
  -> MR.MapFoldT mm x e
basicListF u a r = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold (defaultGatherer @kc (pure @[])) u a)
  r

unpackOnlyFold
  :: forall h mm g x y
   . ( Applicative h
     , Monoid (h y)
     , Traversable g
     , Foldable h
     , Functor (MR.MapFoldT mm x)
     , MR.IdStep mm
     )
  => MR.Unpack mm g x y
  -> MR.MapFoldT mm x (h y)
unpackOnlyFold unpack = MR.mapGatherReduceFold
  (MR.uagMapAllGatherEachFold (MR.defaultOrdGatherer (pure @h))
                              unpack
                              (MR.idAssigner)
  )
  MR.idReducer

parBasicListHashableF
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
parBasicListHashableF oneSparkMax numThreads u a r =
  let g                        = MRP.parReduceGathererHashableL (pure @[])
      (MR.MapGather _ mapStep) = MR.uagMapAllGatherEachFold g u a
  in  MRP.parallelMapReduceF oneSparkMax numThreads g mapStep r


parBasicListOrdF
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
parBasicListOrdF oneSparkMax numThreads u a r =
  let g                        = MRP.parReduceGathererOrd (pure @[])
      (MR.MapGather _ mapStep) = MR.uagMapAllGatherEachFold g u a
  in  MRP.parallelMapReduceF oneSparkMax numThreads g mapStep r

