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
  , resultToList
  -- * groupBy functions
  , groupByHashedKey
  , groupByOrderedKey
  -- * re-exports
  , SerialT
  , WSerialT
  , AheadT
  , AsyncT
  , WAsyncT
  , ParallelT
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import           Data.Functor.Identity          ( Identity )
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

effect :: (Monad m, S.IsStream t, Monad (t m)) => m (t m b) -> t m b
effect = join . S.yieldM -- \x -> S.concatMapM (const x) (S.yield ())

-- This all uses [c] internally and I'd rather it used a Stream there as well.  But when I try to do that, it's slow.
-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashedKey
  :: forall m k c
   . (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, [c])
groupByHashedKey s = effect $ do
  lkc <- S.toList s
  let hm = HMS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByHashedKey #-}

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | group the mapped and assigned values by key using a Data.Map.Strict
groupByOrderedKey
  :: forall m k c
   . (Monad m, Ord k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, [c])
groupByOrderedKey s = effect $ do
  lkc <- S.toList s
  let hm = MS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByOrderedKey #-}

-- | make your stream into a []
resultToList :: (Monad m, S.IsStream t) => t m a -> m [a]
resultToList = S.toList . S.adapt

-- | map-reduce-fold engine builder returning a (Istream t => t Identity d) result
streamlyEngine
  :: forall t y k c x d
   . S.IsStream t
  => (forall z . S.SerialT Identity (k, z) -> S.SerialT Identity (k, [z]))
  -> MRE.MapReduceFold y k c (t Identity) x d
streamlyEngine groupByKey u (MRC.Assign a) r = fmap S.adapt $ FL.Fold
  (flip S.cons)
  S.nil
  ( S.map (\(k, lc) -> MRE.reduceFunction r k lc)
  . groupByKey
  . S.map a
  . unpackStream u
  )
{-# INLINABLE streamlyEngine #-}

-- | effectful map-reduce-fold engine builder returning a (Istream t => t m d) result
-- The "MonadAsync" constraint here more or less requires us ot run in IO or something IO like.
streamlyEngineM
  :: forall t m y k c x d
   . (S.IsStream t, Monad m, S.MonadAsync m)
  => (forall z . S.SerialT m (k, z) -> S.SerialT m (k, [z]))
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
reduceStream :: MRC.Reduce k x d -> k -> Stream (Of x) Identity r -> d
reduceStream (MRC.Reduce     f) k s = runIdentity $ fmap (f k) $ S.toList_ s
reduceStream (MRC.ReduceFold f) k s = runIdentity $ FL.purely S.fold_ (f k) s
{-# INLINABLE reduceStream #-}

reduceStreamM :: Monad m => MRC.ReduceM m k x d -> k -> Stream (Of x) m r -> m d
reduceStreamM (MRC.ReduceM     f) k s = S.toList_ s >>= (f k)
reduceStreamM (MRC.ReduceFoldM f) k s = FL.impurely S.foldM_ (f k) s
{-# INLINABLE reduceStreamM #-}
-}

