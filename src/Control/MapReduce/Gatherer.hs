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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.MapReduce.Gatherer
  ( defaultHashableGatherer
  , defaultOrdGatherer
  , sequenceGatherer
  , gathererSeqToStrictHashMap
  , gathererSeqToLazyHashMap
  , gathererSeqToStrictMap
  , gathererSeqToLazyMap
  )
where

import qualified Control.MapReduce.Core        as MRC

import qualified Control.Foldl                 as FL
import           Data.Foldable                  ( fold )
import qualified Data.Map.Strict               as MS
import qualified Data.Map.Strict               as ML
import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Sequence                 as Seq

import           Data.Hashable                  ( Hashable )


-- this one is fastest in simple tests.  And close to linear, prolly the expected N(ln N)
defaultHashableGatherer
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
defaultHashableGatherer = gathererSeqToStrictHashMap
{-# INLINABLE defaultHashableGatherer #-}

defaultOrdGatherer
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
defaultOrdGatherer = gathererSeqToStrictMap
{-# INLINABLE defaultOrdGatherer #-}


foldToSequence :: Foldable h => h (k, c) -> Seq.Seq (k, c)
foldToSequence = FL.fold (FL.Fold (Seq.|>) Seq.empty id)
{-# INLINABLE foldToSequence #-}

-- fix Seq as the gatherer type, that is the type we use as we unpack and assign but before we group by key
sequenceGatherer
  :: forall mt k d c ce
   . (Semigroup d, Foldable (mt k))
  => ([(k, d)] -> mt k d)
  -> (forall e . (Monoid e, ce e) => (k -> d -> e) -> mt k d -> e)
  -> (  forall e f
      . (Applicative f, Monoid e, ce e)
     => (k -> d -> f e)
     -> mt k d
     -> f (mt k e)
     )
  -> (forall e . (Monoid e, ce e) => mt k e -> e) -- this might not be foldMap in || case
  -> ((c -> d) -> MRC.Gatherer ce (Seq.Seq (k, c)) k c d)
sequenceGatherer fromKeyValueList foldMapWithKey traverseWithKey foldMonoid toSG
  = let seqToMap :: Seq.Seq (k, c) -> mt k d
        seqToMap =
          fromKeyValueList . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list -- can we do this as a direct fold?
    in  MRC.Gatherer
          foldToSequence
          (\f s -> foldMapWithKey f $ seqToMap s)
          (\f s -> fmap foldMonoid . traverseWithKey f $ seqToMap s)
{-# INLINABLE sequenceGatherer #-}

gathererSeqToStrictHashMap
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictHashMap = sequenceGatherer
  (HMS.fromListWith (<>))
  (\f -> HMS.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HMS.traverseWithKey
  fold
{-# INLINABLE gathererSeqToStrictHashMap #-}

gathererSeqToLazyHashMap
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyHashMap = sequenceGatherer
  (HML.fromListWith (<>))
  (\f -> HML.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HML.traverseWithKey
  fold
{-# INLINABLE gathererSeqToLazyHashMap #-}

gathererSeqToStrictMap
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictMap = sequenceGatherer (MS.fromListWith (<>))
                                          MS.foldMapWithKey
                                          MS.traverseWithKey
                                          fold
{-# INLINABLE gathererSeqToStrictMap #-}

gathererSeqToLazyMap
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyMap = sequenceGatherer (ML.fromListWith (<>))
                                        ML.foldMapWithKey
                                        ML.traverseWithKey
                                        fold
{-# INLINABLE gathererSeqToLazyMap #-}

{-
-- This one is slower.  Why?
-- Not exporting because it shouldn't be used.
gathererSeqToSortedList
  :: (Monoid d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToSortedList toSG =
  let seqToList =
        Sort.monoidSortAssocs . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list
  in  Gatherer foldToSequence
               (\f s -> mconcat . fmap (uncurry f) $ seqToList s)
               (\f s -> fmap mconcat . traverse (uncurry f) $ seqToList s)
{-# INLINABLE gathererSeqToSortedList #-}
-}
