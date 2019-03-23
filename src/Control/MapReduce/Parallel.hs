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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
module Control.MapReduce.Parallel
  ( parallelMapReduceF
  , defaultParReduceGatherer
  , parReduceGathererOrd
  , parReduceGathererHashableL
  , parReduceGathererHashableS
  , parFoldMonoid
  , parFoldMonoidDC
  -- * re-exports
  , NFData
  )
where

import qualified Control.MapReduce.Core        as MR
import qualified Control.MapReduce.Gatherer    as MR

import qualified Control.Foldl                 as FL
import qualified Data.Foldable                 as F
import qualified Data.List                     as L
import qualified Data.List.Split               as L
import qualified Data.Sequence                 as Seq
import qualified Data.Map.Strict               as MS
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashMap.Strict           as HML
import           Data.Monoid                    ( Monoid
                                                , mconcat
                                                )

import qualified Control.Parallel.Strategies   as PS
import           Control.Parallel.Strategies    ( NFData ) -- for re-export
--

-- | You can use this in the reguler mapReduce call and it will do parallel reduce.  To get parallelism in the map stage as well,
-- use the parallelMapReduce below.  
defaultParReduceGatherer
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
defaultParReduceGatherer = parReduceGathererHashableL


parallelMapReduceF
  :: forall h x gt k y z ce e
   . ( Monoid e
     , ce e
     , PS.NFData gt
     , Functor (MR.MapFoldT 'Nothing x)
     , Foldable h
     , Monoid gt
     )
  => Int -- 1000 seems optimal on my current machine
  -> Int
  -> MR.Gatherer ce gt k y (h z)
  -> MR.MapStep 'Nothing x gt
  -> MR.Reduce 'Nothing k h z e
  -> MR.MapFoldT 'Nothing x e
parallelMapReduceF oneSparkMax numThreads gatherer mapStep reduceStep =
  let
    chunkedF :: FL.Fold x [[x]] = fmap (L.divvy numThreads numThreads) FL.list -- list divvied into n sublists
    mappedF :: FL.Fold x [gt] =
      fmap (parMapEach (FL.fold (MR.mapFold mapStep))) chunkedF -- list of n gt
    mergedF :: FL.Fold x gt = fmap (parFoldMonoid oneSparkMax) mappedF
    reducedF                = case reduceStep of
      MR.Reduce f -> fmap (MR.gFoldMapWithKey gatherer f) mergedF
      MR.ReduceFold f ->
        fmap (MR.gFoldMapWithKey gatherer (\k hx -> FL.fold (f k) hx)) mergedF
  in
    reducedF
{-# INLINABLE parallelMapReduceF #-}


parMapEach :: PS.NFData b => (a -> b) -> [a] -> [b]
parMapEach = PS.parMap (PS.rparWith PS.rdeepseq)
{-# INLINABLE parMapEach #-}

parMapChunk :: PS.NFData b => Int -> (a -> b) -> [a] -> [b]
parMapChunk chunkSize f =
  PS.withStrategy (PS.parListChunk chunkSize (PS.rparWith PS.rdeepseq)) . fmap f
{-# INLINABLE parMapChunk #-}


-- | Use these in a call to Control.MapReduce.mapReduceFold
-- for parallel reduction.  For parallel folding after reduction, choose a parFoldMonoid f
parReduceGathererOrd'
  :: (Semigroup d, Ord k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)  -- use `foldMap id` for sequential folding or use a parallel foldMonoid from below
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererOrd' foldMonoid = MR.sequenceGatherer
  (MS.fromListWith (<>))
  (\f -> foldMonoid . parMapEach (uncurry f) . MS.toList)
  (\f -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . MS.traverseWithKey f
  )
  foldMonoid
{-# INLINABLE parReduceGathererOrd' #-}

parReduceGathererOrd
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererOrd = parReduceGathererOrd' F.fold
{-# INLINABLE parReduceGathererOrd #-}

parReduceGathererHashableS'
  :: (Semigroup d, Hashable k, Eq k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableS' foldMonoid = MR.sequenceGatherer
  (HMS.fromListWith (<>))
  (\doOne -> foldMonoid . parMapChunk 1000 (uncurry doOne) . HMS.toList)
  (\doOneM -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . HMS.traverseWithKey doOneM
  )
  foldMonoid
{-# INLINABLE parReduceGathererHashableS' #-}

parReduceGathererHashableS
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableS = parReduceGathererHashableS' F.fold
{-# INLINABLE parReduceGathererHashableS #-}

parReduceGathererHashableL'
  :: (Semigroup d, Hashable k, Eq k)
  => (forall e f . (Monoid e, Foldable f) => f e -> e)
  -> (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL' foldMonoid = MR.sequenceGatherer
  (HML.fromListWith (<>))
  (\doOne -> foldMonoid . parMapChunk 1000 (uncurry doOne) . HML.toList)
  (\doOneM -> fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
    . HML.traverseWithKey doOneM
  )
  foldMonoid
{-# INLINABLE parReduceGathererHashableL' #-}


parReduceGathererHashableL
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL = parReduceGathererHashableL' F.fold
{-# INLINABLE parReduceGathererHashableL #-}

{-
parTraverseEach
  :: forall t f a b
   . (PS.NFData (f b), Traversable t, Applicative f)
  => (a -> f b)
  -> t a
  -> f (t b)
parTraverseEach f =
  sequenceA . PS.withStrategy (PS.parTraversable @t PS.rdeepseq) . fmap f
-}

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




{-

parReduceGathererMM
  :: (Semigroup d, Ord k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (MML.MonoidalMap k d) k c d
parReduceGathererMM toSG = Gatherer
  (MML.fromListWith (<>) . fmap (\(k, c) -> (k, toSG c)) . FL.fold FL.list)
  (\doOne -> fold . parMapEach (uncurry doOne) . MML.toList)
  (\doOneM ->
    fmap fold
      . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
      . MML.traverseWithKey doOneM -- hopefully, this just lazily creates thunks
  )





parReduceGathererHashableL
  :: (Semigroup d, Hashable k, Eq k)
  => (c -> d)
  -> MR.Gatherer PS.NFData (Seq.Seq (k, c)) k c d
parReduceGathererHashableL toSG
  = let seqToMap =
          HML.fromListWith (<>)
            . fmap (\(k, c) -> (k, toSG c))
            . FL.fold FL.list
    in
      Gatherer
        (FL.fold (FL.Fold (\s x -> s Seq.|> x) Seq.empty id))
        (\doOne ->
          foldMap id . parMapEach (uncurry doOne) . HML.toList . seqToMap
        )
        (\doOneM ->
          fmap (foldMap id)
            . fmap (PS.withStrategy (PS.parTraversable PS.rdeepseq)) -- deepseq each one in ||
            . HML.traverseWithKey doOneM -- hopefully, this just lazily creates thunks
            . seqToMap
        )
{-# INLINABLE parReduceGathererHashableL #-}
-}
