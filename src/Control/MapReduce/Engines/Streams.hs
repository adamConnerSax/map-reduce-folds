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
module Control.MapReduce.Engines.Streams
  (
    -- * Engines
    streamEngine
  , streamEngineM
  -- * groupBy functions
  , groupByHashedKey
  , groupByOrdKey
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
--import           Control.Monad                  ( join )
import           Data.Bool                      ( bool )
--import qualified Data.List                     as L
import           Data.Functor.Identity          ( Identity(Identity)
                                                , runIdentity
                                                )
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import qualified Data.Profunctor               as P
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


-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashedKey
  :: forall m k c r
   . (Monad m, Hashable k, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByHashedKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = HMS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ HMS.foldrWithKey (\k lc s -> S.cons (k, lc) s) (return r) hm
{-# INLINABLE groupByHashedKey #-}

-- | group the mapped and assigned values by key using Data.Map.Strict
groupByOrdKey
  :: forall m k c r
   . (Monad m, Ord k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, [c])) m r
groupByOrdKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = MS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
  return $ MS.foldrWithKey (\k lc s -> S.cons (k, lc) s) (return r) hm
{-# INLINABLE groupByOrdKey #-}

-- | map-reduce-fold engine builder returning a [] result
streamEngine
  :: (  forall c r
      . Stream (Of (k, c)) Identity r
     -> Stream (Of (k, [c])) Identity r
     )
  -> MRE.MapReduceFold y k c [] x d
streamEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a -> S.cons a s)
  (return ())
  ( runIdentity
  . S.toList_
  . S.map (\(k, lc) -> MRE.reduceFunction r k lc)
  . groupByKey
  . S.map a
  . unpackStream u
  )
{-# INLINABLE streamEngine #-}

-- | effectful map-reduce-fold engine builder returning a [] result
streamEngineM
  :: Monad m
  => (forall c r . Stream (Of (k, c)) m r -> Stream (Of (k, [c])) m r)
  -> MRE.MapReduceFoldM m y k c [] x d
streamEngineM groupByKey u (MRC.AssignM a) r =
  MRC.postMapM id $ FL.generalize $ FL.Fold
    (\s a -> S.cons a s)
    (return ())
    ( S.toList_
    . S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
    . groupByKey
    . S.mapM a
    . unpackStreamM u
    )
-- NB: @postMapM id@ is sneaky.  id :: m d -> m d interpreted as a -> m b implies b ~ d so you get
-- postMapM id (FoldM m x (m d)) :: FoldM m x d
-- which makes more sense if you recall that postMapM f just changes the "done :: x -> m (m d)" step to done' = done >>= f and
-- (>>= id) = join . fmap id = join, so done' :: x -> m d, as we need for the output type.
{-# INLINABLE streamEngineM #-}

-- Unused below.  Leaving here in case I need it later.

-- NB: groupBy Step leaves Stream (Stream (Of (k,c)) m) m r
-- TODO: This feels deeply unidiomatic
-- And I think it's O(N^2). Ick.  There's no binary search here.
gatherStreamNaive
  :: forall m k c r
   . (Monad m, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, Stream (Of c) m ())) m r
gatherStreamNaive = S.catMaybes . S.mapped f . S.groupBy
  (\x y -> (fst x) == (fst y))
 where
  f :: Stream (Of (k, c)) m x -> m (Of (Maybe (k, Stream (Of c) m ())) x)
  f s = do
    (mkc S.:> x) <- S.head s
    case mkc of
      Nothing -> return $ Nothing S.:> x
      Just (k, _) ->
        return $ (Just ((k, fmap (const ()) (S.map snd s)))) S.:> x
{-# INLINABLE gatherStreamNaive #-}

{- This one is bad! 
gatherStream3
  :: forall m k c r
   . (Monad m, Hashable k, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, Stream (Of c) m ())) m r
gatherStream3 s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = HML.fromListWith (<>) $ fmap (second $ S.yield) lkc
  return $ HML.foldrWithKey (\k sc s -> S.cons (k, sc) s) (return r) hm
{-# INLINABLE gatherStream3 #-}
-}


-- | case analysis of Reduce for streaming based mapReduce
reduceStream :: Monad m => MRC.Reduce k x d -> k -> Stream (Of x) m r -> m d
reduceStream (MRC.Reduce     f) k s = fmap (f k) $ S.toList_ s
reduceStream (MRC.ReduceFold f) k s = FL.purely S.fold_ (f k) s
{-# INLINABLE reduceStream #-}

reduceStreamM :: Monad m => MRC.ReduceM m k x d -> k -> Stream (Of x) m r -> m d
reduceStreamM (MRC.ReduceM     f) k s = S.toList_ s >>= (f k)
reduceStreamM (MRC.ReduceFoldM f) k s = FL.impurely S.foldM_ (f k) s
{-# INLINABLE reduceStreamM #-}
