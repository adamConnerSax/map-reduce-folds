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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.Streams
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using @Streaming.Streams@ as its intermediate type.  Because @Streaming.Stream@ does not end with the
type of data data in the @Stream@, we wrap this type in @StreamResult@ for the purposes of the output type of the fold.
-}
module Control.MapReduce.Engines.Streaming
  (
    -- * Helper Types
    StreamResult(..)
    -- * Engines
  , streamingEngine
  , streamingEngineM

  -- * result extractors
  , resultToList
  , concatStream
  , concatStreamFold
  , concatStreamFoldM

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
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map.Strict               as MS
import qualified Data.Sequence                 as Seq
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

-- | case analysis of Unpack for streaming based mapReduce
unpackStreamM
  :: Monad m => MRC.UnpackM m x y -> Stream (Of x) m r -> Stream (Of y) m r
unpackStreamM (MRC.FilterM t) = S.filterM t
unpackStreamM (MRC.UnpackM f) = S.concat . S.mapM f
{-# INLINABLE unpackStreamM #-}

-- | group the mapped and assigned values by key using a @Data.HashMap.Strict@
groupByHashableKey
  :: forall m k c r
   . (Monad m, Hashable k, Eq k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, Seq.Seq c)) m r
groupByHashableKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = HMS.fromListWith (<>) $ fmap (second Seq.singleton) lkc
  return $ HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') (return r) hm
{-# INLINABLE groupByHashableKey #-}

-- | group the mapped and assigned values by key using a @Data.Map.Strict@
groupByOrderedKey
  :: forall m k c r
   . (Monad m, Ord k)
  => Stream (Of (k, c)) m r
  -> Stream (Of (k, Seq.Seq c)) m r
groupByOrderedKey s = S.effect $ do
  (lkc S.:> r) <- S.toList s
  let hm = MS.fromListWith (<>) $ fmap (second Seq.singleton) lkc
  return $ MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') (return r) hm
{-# INLINABLE groupByOrderedKey #-}

-- | Wrap @Stream (Of d) m ()@ in a type which has @d@ as its last parameter
newtype StreamResult m d = StreamResult { unRes :: Stream (Of d) m () }

-- | get a @[]@ result from a Stream
resultToList :: Monad m => StreamResult m d -> m [d]
resultToList = S.toList_ . unRes

concatStreaming :: (Monad m, Monoid a) => Stream (Of a) m () -> m a
concatStreaming = S.iterT g . fmap (const mempty)
  where g (a S.:> ma) = fmap (a <>) ma

-- | @mappend@ all elements of a @StreamResult@ of monoids
concatStream :: (Monad m, Monoid a) => StreamResult m a -> m a
concatStream = concatStreaming . unRes

concatStreamFold
  :: Monoid b => FL.Fold a (StreamResult Identity b) -> FL.Fold a b
concatStreamFold = fmap (S.runIdentity . concatStream)

concatStreamFoldM
  :: (Monad m, Monoid b) => FL.FoldM m a (StreamResult m b) -> FL.FoldM m a b
concatStreamFoldM = MRC.postMapM concatStream

-- | map-reduce-fold engine builder returning a @StreamResult@
streamingEngine
  :: (Foldable g, Functor g)
  => (  forall z r
      . Stream (Of (k, z)) Identity r
     -> Stream (Of (k, g z)) Identity r
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

-- | effectful map-reduce-fold engine builder returning a @StreamResult@
streamingEngineM
  :: (Monad m, Traversable g)
  => (forall z r . Stream (Of (k, z)) m r -> Stream (Of (k, g z)) m r)
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

