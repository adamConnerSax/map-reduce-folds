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
{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE TypeApplications      #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.Streams
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using Streamly streams as its intermediate and return type
NB: These are polymorphic in the return type.  Thought the streams do have to be serial when groupBy is called
So you have to specify the stream type in the call or it has to be inferrable from the use of the result.
-}
module Control.MapReduce.Engines.Streamly
  (
    -- * Engines
    streamlyEngine
  , streamlyEngineM
  , concurrentStreamlyEngine
  , resultToList
  -- * groupBy functions
  , groupByHashableKey
  , groupByOrderedKey
  -- * re-exports
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
import           Data.Functor.Identity          ( Identity
                                                , runIdentity
                                                )
import           Data.Hashable                  ( Hashable )
--import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
--import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import qualified Streamly.Prelude              as S
import qualified Streamly                      as S
import           Streamly                       ( SerialT
                                                , WSerialT
                                                , AheadT
                                                , AsyncT
                                                , WAsyncT
                                                , ParallelT
                                                , MonadAsync
                                                , IsStream
                                                )

-- | case analysis of Unpack for streaming based mapReduce
unpackStream :: S.IsStream t => MRC.Unpack x y -> t Identity x -> t Identity y
unpackStream (MRC.Filter t) = S.filter t
unpackStream (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackStream #-}

-- | case analysis of Unpack for list based mapReduce
unpackStreamM :: (S.IsStream t, Monad m) => MRC.UnpackM m x y -> t m x -> t m y
unpackStreamM (MRC.FilterM t) = S.filter t -- this is a non-effectful filter
unpackStreamM (MRC.UnpackM f) = S.concatMapM (fmap S.fromFoldable . f)
{-# INLINABLE unpackStreamM #-}


-- This all uses [c] internally and I'd rather it used a Stream there as well.  But when I try to do that, it's slow.
-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of (k,c) by hashable key.
-- NB: this function uses the fact that (SerialT m) is a monad
groupByHashableKey
  :: forall m k c
   . (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, [c])
groupByHashableKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = HMS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByHashableKey #-}

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of (k,c) by ordered key.
-- NB: this function uses the fact that (SerialT m) is a monad
groupByOrderedKey
  :: forall m k c
   . (Monad m, Ord k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, [c])
groupByOrderedKey s = do
  lkc <- S.yieldM (S.toList s)
  let hm = MS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByOrderedKey #-}


-- | make your stream into a []
resultToList :: (Monad m, S.IsStream t) => t m a -> m [a]
resultToList = S.toList . S.adapt

-- | map-reduce-fold engine builder returning a @SerialT Identity d@ result
streamlyEngine
  :: forall y k c x d
   . (forall z . S.SerialT Identity (k, z) -> S.SerialT Identity (k, [z]))
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

-- | case analysis of Unpack for streaming based mapReduce
unpackConcurrently
  :: (S.MonadAsync m, S.IsStream t) => MRC.Unpack x y -> t m x -> t m y
unpackConcurrently (MRC.Filter t) = S.filter t
unpackConcurrently (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackConcurrently #-}

-- | possibly concurrent map-reduce-fold engine builder returning an @(Istream t, MonadAsync m) => t m d@ result
concurrentStreamlyEngine
  :: forall tIn tOut m y k c x d
   . (S.IsStream tIn, S.IsStream tOut, S.MonadAsync m)
  => (forall z . S.SerialT m (k, z) -> S.SerialT m (k, [z]))
  -> MRE.MapReduceFold y k c (tOut m) x d
concurrentStreamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a -> (return a) `S.consM` s)
  S.nil
  ( S.mapM (\(k, lc) -> return $ MRE.reduceFunction r k lc)
  . S.adapt @SerialT @tOut -- make it concurrent for reducing
  . groupByKey
  . S.adapt @tIn @SerialT-- make it serial for grouping
  . S.mapM (return . a)
  . (S.|$) (unpackConcurrently u)
  )
{-# INLINABLE concurrentStreamlyEngine #-}


-- | effectful map-reduce-fold engine builder returning a (Istream t => t m d) result
-- The "MonadAsync" constraint here more or less requires us ot run in IO or something IO like.
streamlyEngineM
  :: forall t m y k c x d
   . (S.IsStream t, Monad m, S.MonadAsync m)
  => (forall z . SerialT m (k, z) -> SerialT m (k, [z]))
  -> MRE.MapReduceFoldM m y k c (t m) x d
streamlyEngineM groupByKey u (MRC.AssignM a) r =
  FL.generalize
    $ fmap S.adapt
    $ FL.Fold
        (flip S.cons)
        S.nil
        ( S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
        . groupByKey
        . S.mapM a -- this requires a serial stream.
        . unpackStreamM u
        )
{-# INLINABLE streamlyEngineM #-}


{-
-- | case analysis of Reduce for streaming based mapReduce
reduceStream :: MRC.Reduce k x d -> k -> S.SerialT Identity x -> d
reduceStream (MRC.Reduce     f) k s = runIdentity $ fmap (f k) $ S.toList s
reduceStream (MRC.ReduceFold f) k s = runIdentity $ FL.purely S.foldl' (f k) s
{-# INLINABLE reduceStream #-}

reduceStreamM :: Monad m => MRC.ReduceM m k x d -> k -> S.SerialT m x -> m d
reduceStreamM (MRC.ReduceM     f) k s = S.toList s >>= (f k)
reduceStreamM (MRC.ReduceFoldM f) k s = FL.impurely S.foldlM' (f k) s
{-# INLINABLE reduceStreamM #-}
-}

