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
Module      : Control.MapReduce.Engines.Vector
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using @Vector@ as its intermediate type.
-}
module Control.MapReduce.Engines.Vector
  (
    -- * Engines
    vectorEngine
  , vectorEngineM

  -- * groupBy functions
  , groupByHashableKey
  , groupByOrderedKey

  -- * re-exports
  , toList
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( (<=<) )
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map.Strict               as MS
import qualified Data.Sequence                 as Seq
import qualified Data.Vector                   as V
import           Data.Vector                    ( Vector
                                                , toList
                                                )
import           Control.Arrow                  ( second )

-- | case analysis of @Unpack@ for @Vector@ based mapReduce
unpackVector :: MRC.Unpack x y -> Vector x -> Vector y
unpackVector (MRC.Filter t) = V.filter t
unpackVector (MRC.Unpack f) = V.concatMap (V.fromList . F.toList . f)
{-# INLINABLE unpackVector #-}

-- | case analysis of @Unpack@ for @Vector@ based mapReduce
unpackVectorM :: Monad m => MRC.UnpackM m x y -> Vector x -> m (Vector y)
unpackVectorM (MRC.FilterM t) = V.filterM t
unpackVectorM (MRC.UnpackM f) =
  fmap (V.concatMap id) . traverse (fmap (V.fromList . F.toList) . f)
{-# INLINABLE unpackVectorM #-}


-- | group the mapped and assigned values by key using a @Data.HashMap.Strict@
groupByHashableKey
  :: forall k c . (Hashable k, Eq k) => Vector (k, c) -> Vector (k, Seq.Seq c)
groupByHashableKey v =
  let hm = HMS.fromListWith (<>) $ V.toList $ fmap (second Seq.singleton) v
  in  V.fromList $ HMS.toList hm -- HML.foldrWithKey (\k lc v -> V.snoc v (k,lc)) V.empty hm 
{-# INLINABLE groupByHashableKey #-}

-- | group the mapped and assigned values by key using a @Data.Map.Strict@
groupByOrderedKey
  :: forall k c . Ord k => Vector (k, c) -> Vector (k, Seq.Seq c)
groupByOrderedKey v =
  let hm = MS.fromListWith (<>) $ V.toList $ fmap (second Seq.singleton) v
  in  V.fromList $ MS.toList hm --MS.foldrWithKey (\k lc s -> VS.cons (k,lc) s) VS.empty hm
{-# INLINABLE groupByOrderedKey #-}

-- | map-reduce-fold builder, using @Vector@, returning a @Vector@ result
vectorEngine
  :: (Foldable g, Functor g)
  => (Vector (k, c) -> Vector (k, g c))
  -> MRE.MapReduceFold y k c Vector x d
vectorEngine groupByKey u (MRC.Assign a) r = fmap
  ( V.map (uncurry (MRE.reduceFunction r))
  . groupByKey
  . V.map a
  . unpackVector u
  )
  FL.vector
{-# INLINABLE vectorEngine #-}

-- | effectful map-reduce-fold builder, using @Vector@, returning an effectful @Vector@ result
vectorEngineM
  :: (Monad m, Traversable g)
  => (Vector (k, c) -> Vector (k, g c))
  -> MRE.MapReduceFoldM m y k c Vector x d
vectorEngineM groupByKey u (MRC.AssignM a) r = MRC.postMapM
  ( (traverse (uncurry (MRE.reduceFunctionM r)) =<<)
  . fmap groupByKey
  . (V.mapM a <=< unpackVectorM u)
  )
  (FL.generalize FL.vector)
{-# INLINABLE vectorEngineM #-}
-- NB: If we are willing to constrain to PrimMonad m, then we can use vectorM here which can do in-place updates, etc.



