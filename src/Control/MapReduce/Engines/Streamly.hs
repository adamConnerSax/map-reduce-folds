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
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE BangPatterns          #-}
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
import           Control.Monad.ST              as ST
import qualified Data.Discrimination.Grouping  as DG
import qualified Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(runIdentity) )
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashTable.Class          as HT
import qualified Data.HashTable.ST.Cuckoo      as HTC
import qualified Data.List.NonEmpty            as LNE
import qualified Data.Maybe                    as Maybe 
import qualified Data.Map.Strict               as MS
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
toStreamlyFoldM (FL.FoldM step start done) = SF.mkFold step start done

-- | convert a Control.Foldl Fold into a Streamly.Data.Fold fold
toStreamlyFold :: Monad m => FL.Fold a b -> SF.Fold m a b
toStreamlyFold (FL.Fold step start done) = SF.mkPure step start done


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

-- | mappend all in a monoidal stream
concatStream :: (Monad m, Monoid a) => S.SerialT m a -> m a
concatStream = S.foldl' (<>) mempty

-- | mappend everything in a pure Streamly fold
concatStreamFold :: Monoid b => FL.Fold a (S.SerialT Identity b) -> FL.Fold a b
concatStreamFold = fmap (runIdentity . concatStream)

-- | mappend everything in an effectful Streamly fold.
concatStreamFoldM
  :: (Monad m, Monoid b, S.IsStream t) => FL.FoldM m a (t m b) -> FL.FoldM m a b
concatStreamFoldM = MRC.postMapM (concatStream . S.adapt)

-- | mappend everything in a concurrent Streamly fold.
concatConcurrentStreamFold
  :: (Monad m, Monoid b, S.IsStream t) => FL.Fold a (t m b) -> FL.FoldM m a b
concatConcurrentStreamFold = concatStreamFoldM . FL.generalize

-- | map-reduce-fold builder returning a @SerialT Identity d@ result
streamlyEngine
  :: (Foldable g, Functor g)
  => (forall z . S.SerialT Identity (k, z) -> S.SerialT Identity (k, g z))
  -> MRE.MapReduceFold y k c (SerialT Identity) x d
streamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (flip S.cons)
  S.nil
  ( S.map (\(k, lc) -> MRE.reduceFunction r k lc)
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
  :: forall tIn tOut m g y k c x d
   . (S.IsStream tIn, S.IsStream tOut, S.MonadAsync m, Foldable g, Functor g)
  => (forall z . S.SerialT m (k, z) -> S.SerialT m (k, g z))
  -> MRE.MapReduceFold y k c (tOut m) x d
concurrentStreamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a' -> (return a') `S.consM` s)
  S.nil
  ( S.mapM (\(k, lc) -> return $ MRE.reduceFunction r k lc)
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
  :: (S.IsStream t, Monad m, S.MonadAsync m, Traversable g)
  => (forall z . SerialT m (k, z) -> SerialT m (k, g z))
  -> MRE.MapReduceFoldM m y k c (t m) x d
streamlyEngineM groupByKey u (MRC.AssignM a) r =
  FL.generalize
    $ fmap S.adapt
    $ FL.Fold
        (flip S.cons)
        S.nil
        ( S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
        . groupByKey -- this requires a serial stream.
        . S.mapM a
        . unpackStreamM u
        )
{-# INLINABLE streamlyEngineM #-}

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

-- | Group streamly stream of @(k,c)@ by @hashable@ key. Uses mutable hashtables running in the ST monad.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKeyST
  :: (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByHashableKeyST st = do
  lkc <- S.yieldM (S.toList st)
  ST.runST $ do
    hm <- (MRE.fromListWithHT @HTC.HashTable) (<>)
      $ fmap (second Seq.singleton) lkc
    HT.foldM (\s' (k, sc) -> return $ S.cons (k, sc) s') S.nil hm
{-# INLINABLE groupByHashableKeyST #-}


-- | Group streamly stream of @(k,c)@ by key with instance of Grouping from <http://hackage.haskell.org/package/discrimination>.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByDiscriminatedKey
  :: (Monad m, DG.Grouping k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByDiscriminatedKey s = do
  lkc <- S.yieldM (S.toList s)
  let g :: LNE.NonEmpty (k, c) -> (k, Seq.Seq c)
      g x = let k = fst (LNE.head x) in (k, F.fold $ fmap (Seq.singleton . snd) x)
  S.fromFoldable $ Maybe.catMaybes . fmap (fmap g . LNE.nonEmpty) $ DG.groupWith fst lkc
{-# INLINABLE groupByDiscriminatedKey #-}
