{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.Streams
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using @Streamly@ streams as its intermediate and return type.

Notes:
1. These are polymorphic in the return stream type.  Thought the streams do have to be @serial@ when @groupBy@ is called
So you have to specify the stream type in the call or it has to be inferrable from the use of the result.

2. There is a concurrent engine here, one that uses Streamly's concurrency features to map over the stream.  I've not
been able to verify that this is faster on an appropriate task with appropriate runtime settings.
-}
module Control.MapReduce.Engines.Streamly
  (
    -- * Engines
    streamlyEngine
  , streamlyEngineM
  , concurrentStreamlyEngine

    -- * Streamly Combinators
  , toStreamlyFold
  , toStreamlyFoldM

    -- * Result Extraction
  , resultToList
  , concatStream
  , concatStreamFold
  , concatStreamFoldM
  , concatConcurrentStreamFold

    -- * @groupBy@ Functions
  , groupByHashableKey
  , groupByOrderedKey
  , groupByHashableKeyST
  , groupByDiscriminatedKey

    -- * Re-Exports
  , SerialT
  , WSerialT
  , AheadT
  , AsyncT
  , WAsyncT
  , ParallelT
  , MonadAsync
  , IsStream
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import           Control.Monad.ST              as ST
import qualified Data.Discrimination.Grouping  as DG
import qualified Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(runIdentity) )
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashTable.Class          as HT
import qualified Data.HashTable.ST.Cuckoo      as HTC
import qualified Data.Map.Strict               as MS
import           Data.Monoid                    ( Endo(..) )
import qualified Data.Sequence                 as Seq
import qualified Streamly.Prelude              as S
import qualified Streamly                      as S
import qualified Streamly.Internal.Data.Fold   as SF
import           Streamly                       ( SerialT
                                                , WSerialT
                                                , AheadT
                                                , AsyncT
                                                , WAsyncT
                                                , ParallelT
                                                , MonadAsync
                                                , IsStream
                                                )

-- | convert a Control.Foldl FoldM into a Streamly.Data.Fold fold
toStreamlyFoldM :: FL.FoldM m a b -> SF.Fold m a b
toStreamlyFoldM (FL.FoldM step start done) = SF.Fold step start done
{-# INLINABLE toStreamlyFoldM #-}

-- | convert a Control.Foldl Fold into a Streamly.Data.Fold fold
toStreamlyFold :: Monad m => FL.Fold a b -> SF.Fold m a b
toStreamlyFold (FL.Fold step start done) = SF.mkPure step start done
{-# INLINABLE toStreamlyFold #-}

streamlyReduce
  :: forall m k x d . Monad m => MRC.Reduce k x d -> k -> S.SerialT m x -> m d
streamlyReduce (MRC.Reduce     f) k = fmap (f k) . S.toList
streamlyReduce (MRC.ReduceFold f) k = S.fold (toStreamlyFold (f k))
{-# INLINABLE streamlyReduce #-}

streamlyReduceM
  :: forall m k x d
   . Monad m
  => MRC.ReduceM m k x d
  -> k
  -> S.SerialT m x
  -> m d
streamlyReduceM (MRC.ReduceM     f) k = join . fmap (f k) . S.toList
streamlyReduceM (MRC.ReduceFoldM f) k = S.fold (toStreamlyFoldM (f k))
{-# INLINABLE streamlyReduceM #-}
--streamlyReduceM ::  


-- | unpack for streamly based map/reduce
unpackStream :: S.IsStream t => MRC.Unpack x y -> t Identity x -> t Identity y
unpackStream (MRC.Filter t) = S.filter t
unpackStream (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackStream #-}

-- | effectful (monadic) unpack for streamly based map/reduce
unpackStreamM :: (S.IsStream t, Monad m) => MRC.UnpackM m x y -> t m x -> t m y
unpackStreamM (MRC.FilterM t) = S.filterM t
unpackStreamM (MRC.UnpackM f) = S.concatMapM (fmap S.fromFoldable . f)
{-# INLINABLE unpackStreamM #-}

-- | make a stream into an (effectful) @[]@
resultToList :: (Monad m, S.IsStream t) => t m a -> m [a]
resultToList = S.toList . S.adapt
{-# INLINABLE resultToList #-}

-- | mappend all in a monoidal stream
concatStream :: (Monad m, Monoid a) => S.SerialT m a -> m a
concatStream = S.foldl' (<>) mempty
{-# INLINABLE concatStream #-}

-- | mappend everything in a pure Streamly fold
concatStreamFold :: Monoid b => FL.Fold a (S.SerialT Identity b) -> FL.Fold a b
concatStreamFold = fmap (runIdentity . concatStream)
{-# INLINABLE concatStreamFold #-}

-- | mappend everything in an effectful Streamly fold.
concatStreamFoldM
  :: (Monad m, Monoid b, S.IsStream t) => FL.FoldM m a (t m b) -> FL.FoldM m a b
concatStreamFoldM = MRC.postMapM (concatStream . S.adapt)
{-# INLINABLE concatStreamFoldM #-}

-- | mappend everything in a concurrent Streamly fold.
concatConcurrentStreamFold
  :: (Monad m, Monoid b, S.IsStream t) => FL.Fold a (t m b) -> FL.FoldM m a b
concatConcurrentStreamFold = concatStreamFoldM . FL.generalize
{-# INLINABLE concatConcurrentStreamFold #-}

-- | map-reduce-fold builder returning a @SerialT Identity d@ result
streamlyEngine
  :: forall y k c x d
   . ({-Foldable g, Functor g-})
  => (forall z . S.SerialT Identity (k, z) -> S.SerialT Identity (k, S.SerialT Identity z))
  -> MRE.MapReduceFold y k c (SerialT Identity) x d
streamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (flip S.cons)
  S.nil
  ( S.map (runIdentity . (uncurry $ streamlyReduce r))--(MRE.uncurryReduceFold $ MRE.reduceFoldVia FL.list r)
  . groupByKey
  . S.map a
  . unpackStream u
  )
{-# INLINABLE streamlyEngine #-}

-- | unpack for concurrent streamly based map/reduce
unpackConcurrently
  :: (S.MonadAsync m, S.IsStream t) => MRC.Unpack x y -> t m x -> t m y
unpackConcurrently (MRC.Filter t) = S.filter t
unpackConcurrently (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackConcurrently #-}

-- | possibly (depending on chosen stream types) concurrent map-reduce-fold builder returning an @(Istream t, MonadAsync m) => t m d@ result
concurrentStreamlyEngine
  :: forall tIn tOut m y k c x d
   . (S.IsStream tIn, S.IsStream tOut, S.MonadAsync m {-,Foldable g, Functor g-})
  => (forall z . S.SerialT m (k, z) -> S.SerialT m (k, S.SerialT m z))
  -> MRE.MapReduceFold y k c (tOut m) x d
concurrentStreamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a' -> (return a') `S.consM` s)
  S.nil
  ( S.mapM (\(k, lc) -> streamlyReduce r k lc)  --(return . (MRE.uncurryReduceFold $ MRE.reduceFoldVia FL.list r))
  . S.adapt @SerialT @tOut -- make it concurrent for reducing
  . groupByKey
  . S.adapt @tIn @SerialT-- make it serial for grouping
  . S.mapM (return . a)
  . (S.|$) (unpackConcurrently u)
  )
{-# INLINABLE concurrentStreamlyEngine #-}

-- | effectful map-reduce-fold engine returning a (Istream t => t m d) result
-- The "MonadAsync" constraint here more or less requires us to run in IO, or something IO like.
streamlyEngineM
  :: forall t g m y k c x d
   . (S.IsStream t, Monad m, S.MonadAsync m, Traversable g)
  => (forall z . SerialT m (k, z) -> SerialT m (k, g z))
  -> MRE.MapReduceFoldM m y k c (t m) x d
streamlyEngineM groupByKey u (MRC.AssignM a) r =
  FL.generalize
    $ fmap S.adapt
    $ FL.Fold
        (flip S.cons)
        S.nil
        ( S.mapM (MRE.uncurryReduceFoldM $ MRE.reduceFoldMViaPure FL.list r)
        . groupByKey -- this requires a serial stream.
        . S.mapM a
        . unpackStreamM u
        )
{-# INLINABLE streamlyEngineM #-}

toStreamF :: forall t m a . (Monad m, S.IsStream t) => FL.Fold a (t m a)
toStreamF = FL.Fold (flip S.cons) S.nil id

-- Church-encoded stream building

-- | Group streamly stream of @(k,c)@ by @hashable@ key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKey
  :: forall t m k c
   . (Monad m, Hashable k, Eq k, S.IsStream t)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, t m c)
groupByHashableKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = fmap (($ S.nil) . appEndo) $ HMS.fromListWith (<>) $ fmap
        (second $ Endo . S.cons)
        lkc
  HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByHashableKey #-}

-- Unused.
-- Because it's inexplicably slower than lifting toList into the SerialT m monad,
-- and running HM.fwomListWith on that
-- I'm assuming there's some list fusion happen but still shouldn't the direct fold into the hashmap be faster??
hmViaFold
  :: (Hashable k, Eq k, Monad m)
  => S.SerialT m (k, c)
  -> m (HMS.HashMap k (Seq.Seq c))
hmViaFold s =
  S.foldr (\(k, c) -> HMS.insertWith (<>) k (Seq.singleton c)) HMS.empty s

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of @(k,c)@ by ordered key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByOrderedKey
  :: forall m k c
   . (Monad m, Ord k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, S.SerialT m c)
groupByOrderedKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = fmap (($ S.nil) . appEndo) $ MS.fromListWith (<>) $ fmap
        (second $ Endo . S.cons)
        lkc
  MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByOrderedKey #-}

{-
-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of @(k,c)@ by @hashable@ key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKey
  :: (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByHashableKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = HMS.fromListWith (<>) $ fmap (second $ Seq.singleton) lkc
  HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByHashableKey #-}

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of @(k,c)@ by ordered key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByOrderedKey
  :: (Monad m, Ord k) => S.SerialT m (k, c) -> S.SerialT m (k, Seq.Seq c)
groupByOrderedKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = MS.fromListWith (<>) $ fmap (second $ Seq.singleton) lkc
  MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByOrderedKey #-}
-}

-- | Group streamly stream of @(k,c)@ by @hashable@ key. Uses mutable hashtables running in the ST monad.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKeyST
  :: (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, S.SerialT m c)
groupByHashableKeyST st = do
  lkc <- S.yieldM (S.toList st)
  ST.runST $ do
    hm <- (MRE.fromListWithHT @HTC.HashTable) (<>) $ fmap (second S.yield) lkc
    HT.foldM (\s' (k, sc) -> return $ S.cons (k, sc) s') S.nil hm
{-# INLINABLE groupByHashableKeyST #-}


-- | Group streamly stream of @(k,c)@ by key with instance of Grouping from <http://hackage.haskell.org/package/discrimination>.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByDiscriminatedKey
  :: (Monad m, DG.Grouping k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, S.SerialT m c)
groupByDiscriminatedKey s = do
  lkc <- S.yieldM (S.toList s)
  let g :: [(k, c)] -> (k, S.SerialT m c)
      g x = let k = fst (head x) in (k, F.fold $ fmap (S.yield . snd) x)
  S.fromFoldable $ fmap g $ DG.groupWith fst lkc
{-# INLINABLE groupByDiscriminatedKey #-}
