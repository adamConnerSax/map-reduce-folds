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

map-reduce engine (fold builder) using Streams as its intermediate type.
-}
module Control.MapReduce.Engines.Streaming
  (
    -- * Engines
    streamingEngine
  , streamingEngineM
  , resultToList
  -- * groupBy functions
  , groupByHashableKey
  , groupByOrderedKey
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
import           Data.Functor.Identity          ( Identity )
import           Data.Hashable                  ( Hashable )
--import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
--import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import qualified Streaming.Prelude             as S
import qualified Streaming                     as S
import           Streaming                      ( Stream
                                                , Of
                                                )
import           Control.Arrow                  ( second )


-- | case analysis of Unpack for streaming based mapReduce
unpackStream
  :: MRC.Unpack x y -> S.Stream (Of x) Identity r -> Stream (Of y) Identity r
unpackStream (MRC.Filter t) = S.filter t
unpackStream (MRC.Unpack f) = S.concat . S.map f
{-# INLINABLE unpackStream #-}

-- | case analysis of Unpack for list based mapReduce
unpackStreamM
  :: Monad m => MRC.UnpackM m x y -> Stream (Of x) m r -> Stream (Of y) m r
unpackStreamM (MRC.FilterM t) = S.filter t
unpackStreamM (MRC.UnpackM f) = S.concat . S.mapM f
{-# INLINABLE unpackStreamM #-}

-- This all uses [c] internally and I'd rather it used a Stream there as well.  But when I try to do that, it's slow.
-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashableKey
  :: forall m k c r
   . (Monad m, Hashable k, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByHashableKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = HMS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') (return r) hm
{-# INLINABLE groupByHashableKey #-}

-- | group the mapped and assigned values by key using a Data.Map.Strict
groupByOrderedKey
  :: forall m k c r
   . (Monad m, Ord k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByOrderedKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = MS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') (return r) hm
{-# INLINABLE groupByOrderedKey #-}

newtype StreamResult m d = StreamResult { unRes :: Stream (Of d) m () }
resultToList :: Monad m => StreamResult m d -> m [d]
resultToList = S.toList_ . unRes

-- | map-reduce-fold engine builder returning a Stream result
streamingEngine
  :: (  forall z r
      . Stream (Of (k, z)) Identity r
     -> Stream (Of (k, [z])) Identity r
     )
  -> MRE.MapReduceFold y k c (StreamResult Identity) x d
streamingEngine groupByKey u (MRC.Assign a) r = fmap StreamResult $ FL.Fold
  (flip S.cons)
  (return ())
  ( S.map (\(k, lc) -> MRE.reduceFunction r k lc)
  . groupByKey
  . S.map a
  . unpackStream u
  )
{-# INLINABLE streamingEngine #-}

-- | effectful map-reduce-fold engine builder returning a StreamResult
streamingEngineM
  :: Monad m
  => (forall z r . Stream (Of (k, z)) m r -> Stream (Of (k, [z])) m r)
  -> MRE.MapReduceFoldM m y k c (StreamResult m) x d
streamingEngineM groupByKey u (MRC.AssignM a) r =
  fmap StreamResult . FL.generalize $ FL.Fold
    (flip S.cons)
    (return ())
    ( S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
    . groupByKey
    . S.mapM a
    . unpackStreamM u
    )
{-# INLINABLE streamingEngineM #-}

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

{-
groupByHashedKey
  :: forall k c r
   . (Hashable k, Eq k)
  => Stream (Of (k, c)) Identity r
  -> Stream (Of (k, [c])) Identity r
groupByHashedKey s =
  S.unfoldr (return . unFoldStep) $ second HMS.toList $ S.destroy
    s
    construct
    runIdentity
    (\r -> (r, HMS.empty))
 where
  construct :: Of (k, c) (r, HMS.HashMap k [c]) -> (r, HMS.HashMap k [c])
  construct ((k, c) S.:> (r, t)) = (r, HMS.insertWith (<>) k [c] t)
  unFoldStep :: (r, [(k, x)]) -> (Either r ((k, x), (r, [(k, x)])))
  unFoldStep (r, []    ) = Left r
  unFoldStep (r, x : xs) = Right (x, (r, xs))
{-# INLINABLE groupByHashedKey #-}

groupByHashedKeyM
  :: forall k c r m
   . (Monad m, Hashable k, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByHashedKeyM s =
  S.effect
    $ fmap (S.unfoldr (return . unFoldStep) . second HMS.toList)
    $ S.destroy s construct join (\r -> return (r, HMS.empty))
 where
  construct :: Of (k, c) (m (r, HMS.HashMap k [c])) -> m (r, HMS.HashMap k [c])
  construct ((k, c) S.:> mrt) = fmap (second $ HMS.insertWith (<>) k [c]) mrt
  unFoldStep :: (r, [(k, x)]) -> (Either r ((k, x), (r, [(k, x)])))
  unFoldStep (r, []    ) = Left r
  unFoldStep (r, x : xs) = Right (x, (r, xs))
{-# INLINABLE groupByHashedKeyM #-}

-- | group the mapped and assigned values by key using a Data.Map.Strict
groupByOrderedKey
  :: forall k c r
   . Ord k
  => Stream (Of (k, c)) Identity r
  -> Stream (Of (k, [c])) Identity r
groupByOrderedKey s = S.unfoldr (return . unFoldStep)
  $ S.destroy s construct runIdentity (\r -> (r, MS.empty))
 where
  construct :: Of (k, c) (r, MS.Map k [c]) -> (r, MS.Map k [c])
  construct ((k, c) S.:> (r, t)) = (r, MS.insertWith (<>) k [c] t)
  unFoldStep :: (r, MS.Map k x) -> (Either r ((k, x), (r, MS.Map k x)))
  unFoldStep (r, t) =
    if MS.null t then Left r else Right (MS.elemAt 0 t, (r, MS.drop 1 t))
{-# INLINABLE groupByOrderedKey #-}

groupByOrderedKeyM
  :: forall k c r m
   . (Monad m, Ord k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByOrderedKeyM s =
  S.effect $ fmap (S.unfoldr (return . unFoldStep)) $ S.destroy
    s
    construct
    join
    (\r -> return (r, MS.empty))
 where
  construct :: Of (k, c) (m (r, MS.Map k [c])) -> m (r, MS.Map k [c])
  construct ((k, c) S.:> mrt) = fmap (second $ MS.insertWith (<>) k [c]) mrt
  unFoldStep :: (r, MS.Map k x) -> (Either r ((k, x), (r, MS.Map k x)))
  unFoldStep (r, t) =
    if MS.null t then Left r else Right (MS.elemAt 0 t, (r, MS.drop 1 t))
{-# INLINABLE groupByOrderedKeyM #-}
-}
