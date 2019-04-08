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

map-reduce engine (fold builder) using Vector as its intermediate type.
-}
module Control.MapReduce.Engines.Vector
  (
    -- * Engines
    vectorEngine
  , vectorEngineM
  -- * groupBy functions
  , groupByHashedKey
  , groupByOrdKey
  -- * re-exports
  , toList
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
import           Data.Bool                      ( bool )
import           Data.Functor.Identity          ( Identity(Identity)
                                                , runIdentity
                                                )
import           Control.Monad                  ( (>=>)
                                                , (<=<)
                                                , join
                                                )
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS
import qualified Data.Profunctor               as P
import qualified Data.Vector                   as V
import           Data.Vector                    ( Vector
                                                , toList
                                                )
import           Control.Arrow                  ( second )



-- | case analysis of Unpack for streaming based mapReduce
unpackVector :: MRC.Unpack x y -> Vector x -> Vector y
unpackVector (MRC.Filter t) = V.filter t
unpackVector (MRC.Unpack f) = V.concatMap (V.fromList . F.toList . f)
{-# INLINABLE unpackVector #-}

-- | case analysis of Unpack for list based mapReduce
unpackVectorM :: Monad m => MRC.UnpackM m x y -> Vector x -> m (Vector y)
unpackVectorM (MRC.FilterM t) = return . V.filter t
unpackVectorM (MRC.UnpackM f) =
  fmap (V.concatMap id) . traverse (fmap (V.fromList . F.toList) . f)
{-# INLINABLE unpackVectorM #-}


-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashedKey
  :: forall k c . (Hashable k, Eq k) => Vector (k, c) -> Vector (k, [c])
groupByHashedKey v =
  let hm = HML.fromListWith (<>) $ V.toList $ fmap (second $ pure @[]) v
  in  V.fromList $ HML.toList hm -- HML.foldrWithKey (\k lc v -> V.snoc v (k,lc)) V.empty hm 
{-# INLINABLE groupByHashedKey #-}

-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByOrdKey :: forall k c . Ord k => Vector (k, c) -> Vector (k, [c])
groupByOrdKey v =
  let hm = MS.fromListWith (<>) $ V.toList $ fmap (second $ pure @[]) v
  in  V.fromList $ MS.toList hm --MS.foldrWithKey (\k lc s -> VS.cons (k,lc) s) VS.empty hm
{-# INLINABLE groupByOrdKey #-}

-- | map-reduce-fold engine builder, using Vector.Fusion.Stream.Monadic, returning a Vector result
vectorEngine
  :: (Vector (k, c) -> Vector (k, [c])) -> MRE.MapReduceFold y k c Vector x d
vectorEngine groupByKey u (MRC.Assign a) r = fmap
  ( V.map (uncurry (MRE.reduceFunction r))
  . groupByKey
  . V.map a
  . unpackVector u
  )
  FL.vector
{-# INLINABLE vectorEngine #-}

-- | effectful map-reduce-fold engine builder, using Vector.Fusion.Stream.Monadic, returning a Vector result
vectorEngineM
  :: Monad m
  => (Vector (k, c) -> Vector (k, [c]))
  -> MRE.MapReduceFoldM m y k c Vector x d
vectorEngineM groupByKey u (MRC.AssignM a) r = MRC.postMapM
  ( (traverse (uncurry (MRE.reduceFunctionM r)) =<<)
  . fmap groupByKey
  . (V.mapM a <=< unpackVectorM u)
  )
  (FL.generalize FL.vector)
{-# INLINABLE vectorEngineM #-}
-- NB: If we are willing to constrain to PrimMonad m, then we can use vectorM here



