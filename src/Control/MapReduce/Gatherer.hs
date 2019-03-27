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
Module      : Control.MapReduce.Gatherer
Description : Gatherer code for map-reduce-folds
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

The 'Gatherer' type encapsulates the tasks of gathering and grouping mapped data by key.
It must support two operations, one which takes foldable of (key,value) pairs and maps them
to a monoid container (like [] or Data.Sequence.Seq) and then another that allows doing a
foldMapWithKey over those pairs with a function 'Monoid e => key -> value -> e'
Because the library supports monoidal and non-monoidal variants, the gatherer must
have foldMapWithKey operations of both sorts.
This module contains some gatherer implementations based on using Data.Sequence.Seq as the internal
gathering type and strict and lazy maps and hashmaps as grouping tools.

Gatherers need to know how to map from the value type of assignment to the gathering monoid.  For example,
for [] and Seq, this is just 'pure' applied at the appropriate type. 
-}
module Control.MapReduce.Gatherer
  (
    -- * Default Gatherers
    defaultHashableGatherer
  , defaultOrdGatherer
  -- * build a gatherer using sequence as the internal gathering type
  , sequenceGatherer
  -- * Some gatherers which might be useful
  , gathererSeqToStrictHashMap
  , gathererSeqToLazyHashMap
  , gathererSeqToStrictMap
  , gathererSeqToLazyMap
  , gathererListToLazyHashMap
  )
where

import qualified Control.MapReduce.Core        as MRC

import           Control.Arrow                  ( second )
import           Data.Foldable                  ( fold
                                                , toList
                                                )
import qualified Data.Map.Strict               as MS
import qualified Data.Map.Strict               as ML
import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import           Data.Monoid                    ( (<>)
                                                , Monoid(..)
                                                )
import qualified Data.Sequence                 as Seq

import           Data.Hashable                  ( Hashable )


-- | Default gatherer for Hashable keys
defaultHashableGatherer
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
defaultHashableGatherer = gathererSeqToStrictHashMap
{-# INLINABLE defaultHashableGatherer #-}

-- | Default gatherer for Ord keys
defaultOrdGatherer
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
defaultOrdGatherer = gathererSeqToStrictMap
{-# INLINABLE defaultOrdGatherer #-}


buildPairGatherer :: forall h mt k d c ce . (Semigroup d, Foldable h, Functor h)
  => (forall q. Foldable q => q (k,c) -> h (k,c))                         -- ^ foldable to unsorted storage of (k,c) pairs
  -> (forall h . (Foldable h, Functor h) => h (k, d) -> mt k d)    -- ^ unsorted storage to grouped type
  -> (forall e . (Monoid e, ce e) => (k -> d -> e) -> mt k d -> e) -- ^ foldWithKey 
  -> (  forall e f                                                 -- ^ traverseWithKey
      .                                                  
        (Applicative f, Monoid e, ce e)
     => (k -> d -> f e)
     -> mt k d
     -> f (mt k e)
     )
  -> (forall e . (Monoid e, ce e) => mt k e -> e) -- ^ foldMap-like function.  This is here as a hook for a parallel fold
  -> ((c -> d) -> MRC.Gatherer ce (h (k,c)) k c d)
buildPairGatherer keyedValuesFromFoldable groupFromKeyedValues foldWithKey traverseWithKey foldMonoid toSG =
  let toGrouped :: h (k,c) -> mt k d
      toGrouped = groupFromKeyedValues . fmap (second toSG)
  in MRC.Gatherer
     keyedValuesFromFoldable
     (\f !s -> foldWithKey f $ toGrouped s)
     (\f !s -> fmap foldMonoid . traverseWithKey f $ toGrouped s)
{-# INLINABLE buildPairGatherer #-}
      

-- | Helper function for making any foldable into a sequence
foldToSequence :: Foldable h => h x -> Seq.Seq x
foldToSequence = Seq.fromList . toList --FL.fold (FL.Fold (Seq.|>) Seq.empty id)
{-# INLINABLE foldToSequence #-}



-- TODO: Can I make Discrimination work here?
-- | Build a Data.Sequence.Seq based gatherer.  This specifies the gathering type but lets the user specify how things are to
-- be grouped and sorted. Basically allows sequence based gathering using all sorts of maps to do the grouping.  
sequenceGatherer
  :: forall mt k d c ce
   . (Semigroup d, Foldable (mt k))
  => (forall h . (Foldable h, Functor h) => h (k, d) -> mt k d) -- ^ Seq (k,d) -> Map k d
  -> (forall e . (Monoid e, ce e) => (k -> d -> e) -> mt k d -> e) -- ^ something like foldWithKey but might depend on map type
  -> (  forall e f
      .                                                  -- ^ applicative foldWithKey
        (Applicative f, Monoid e, ce e)
     => (k -> d -> f e)
     -> mt k d
     -> f (mt k e)
     )
  -> (forall e . (Monoid e, ce e) => mt k e -> e) -- ^ foldMap-like function.  This is here as a hook for a parallel fold
  -> ((c -> d) -> MRC.Gatherer ce (Seq.Seq (k, c)) k c d)
sequenceGatherer = buildPairGatherer foldToSequence 

listGatherer :: forall mt k d c ce
                . (Semigroup d, Foldable (mt k))
  => (forall h . (Foldable h, Functor h) => h (k, d) -> mt k d) -- ^ Seq (k,d) -> Map k d
  -> (forall e . (Monoid e, ce e) => (k -> d -> e) -> mt k d -> e) -- ^ something like foldWithKey but might depend on map type
  -> (  forall e f
      .                                                  -- ^ applicative foldWithKey
        (Applicative f, Monoid e, ce e)
     => (k -> d -> f e)
     -> mt k d
     -> f (mt k e)
     )
  -> (forall e . (Monoid e, ce e) => mt k e -> e) -- ^ foldMap-like function.  This is here as a hook for a parallel fold
  -> ((c -> d) -> MRC.Gatherer ce [(k,c)] k c d)
listGatherer = buildPairGatherer toList

{-

  = let seqToMap :: Seq.Seq (k, c) -> mt k d
        seqToMap = fromKeyedValues . fmap (second toSG)
    in  MRC.Gatherer
          foldToSequence
          (\f !s -> foldMapWithKey f $ seqToMap s)
          (\f !s -> fmap foldMonoid . traverseWithKey f $ seqToMap s)
-}
{-# INLINABLE sequenceGatherer #-}


-- | Seq based gatherer using a strict 'HashMap' underneath
gathererSeqToStrictHashMap
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictHashMap = sequenceGatherer
  (HMS.fromListWith (<>) . toList)
  (\f -> HMS.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HMS.traverseWithKey
  fold
{-# INLINABLE gathererSeqToStrictHashMap #-}

-- | Seq based gatherer using a lazy 'HashMap' underneath
gathererSeqToLazyHashMap
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyHashMap = sequenceGatherer
  (HML.fromListWith (<>) . toList)
  (\f -> HML.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HML.traverseWithKey
  fold
{-# INLINABLE gathererSeqToLazyHashMap #-}

-- | Seq based gatherer using a strict 'Map' underneath
gathererSeqToStrictMap
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToStrictMap = sequenceGatherer (MS.fromListWith (<>) . toList)
                                          MS.foldMapWithKey
                                          MS.traverseWithKey
                                          fold
{-# INLINABLE gathererSeqToStrictMap #-}

-- | Seq based gatherer using a lazy 'Map' underneath
gathererSeqToLazyMap
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty (Seq.Seq (k, c)) k c d
gathererSeqToLazyMap = sequenceGatherer (ML.fromListWith (<>) . toList)
                                        ML.foldMapWithKey
                                        ML.traverseWithKey
                                        fold
{-# INLINABLE gathererSeqToLazyMap #-}


-- | Seq based gatherer using a lazy 'HashMap' underneath
gathererListToLazyHashMap
  :: (Monoid d, Hashable k, Eq k)
  => (c -> d)
  -> MRC.Gatherer MRC.Empty [(k, c)] k c d
gathererListToLazyHashMap = listGatherer
  (HML.fromListWith (<>) . toList)
  (\f -> HML.foldlWithKey' (\e k d -> e <> f k d) mempty)
  HML.traverseWithKey
  fold
{-# INLINABLE gathererListToLazyHashMap #-}


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
