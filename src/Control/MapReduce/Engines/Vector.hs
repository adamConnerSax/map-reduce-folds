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

map-reduce engine (fold builder) using Vector.Fusion.Stream as its intermediate type.
-}
module Control.MapReduce.Engines.Vector
  (
    -- * Engines
--    vectorEngine
--  , vectorEngineM
  -- * groupBy functions
    groupByHashedKey
  , groupByOrdKey
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
import           Data.Bool                      ( bool )
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
import qualified Data.Vector.Fusion.Stream.Monadic
                                               as VS
import           Data.Vector.Fusion.Stream.Monadic
                                                ( Stream(Stream) )

import           Control.Arrow                  ( second )



-- | case analysis of Unpack for streaming based mapReduce
unpackVStream :: MRC.Unpack x y -> Stream Identity x -> Stream Identity y
unpackVStream (MRC.Filter t) = VS.filter t
unpackVStream (MRC.Unpack f) = VS.concatMap (VS.fromList . F.toList . f) -- can we do without the [] here?  Should get fused away...
{-# INLINABLE unpackVStream #-}

-- | case analysis of Unpack for list based mapReduce
unpackVStreamM :: Monad m => MRC.UnpackM m x y -> Stream m x -> Stream m y
unpackVStreamM (MRC.FilterM t) = VS.filter t
unpackVStreamM (MRC.UnpackM f) =
  VS.concatMapM (fmap (VS.fromList . F.toList) . f)
{-# INLINABLE unpackVStreamM #-}

fromMonadicList :: forall m a . Monad m => m [a] -> Stream m a
fromMonadicList ma = Stream step ma
 where
  step :: m [a] -> m (VS.Step (m [a]) a)
  step mla = do
    la <- mla
    case la of
      (x : xs) -> return (VS.Yield x (return xs))
      []       -> return VS.Done
{-# INLINABLE fromMonadicList #-}

-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashedKey
  :: forall m k c
   . (Monad m, Hashable k, Eq k)
  => Stream m (k, c)
  -> Stream m (k, [c])
groupByHashedKey s = fromMonadicList $ do
  lkc <- VS.toList s
  return $ HMS.toList $ HMS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
{-# INLINABLE groupByHashedKey #-}

-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByOrdKey
  :: forall m k c . (Monad m, Ord k) => Stream m (k, c) -> Stream m (k, [c])
groupByOrdKey s = fromMonadicList $ do
  lkc <- VS.toList s
  return $ MS.toList $ MS.fromListWith (<>) $ fmap (second $ pure @[]) lkc
{-# INLINABLE groupByOrdKey #-}

{-
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


-}
