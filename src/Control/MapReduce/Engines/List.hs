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
Module      : Control.MapReduce.Engines.List
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using [] as its intermediate type.
-}
module Control.MapReduce.Engines.List
  (
  -- * Engines
    listEngine
  , listEngineM

  -- * @groupBy@ Functions
  , groupByHashableKey
  , groupByOrderedKey

  -- * Helpers
  , unpackList
  , unpackListM
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map.Strict               as MS
import qualified Data.Sequence                 as Seq
import           Control.Monad                  ( filterM )
import           Control.Arrow                  ( second )



-- | unpack for list based map/reduce
unpackList :: MRC.Unpack x y -> [x] -> [y]
unpackList (MRC.Filter t) = L.filter t
unpackList (MRC.Unpack f) = L.concatMap (F.toList . f)
{-# INLINABLE unpackList #-}

-- | effectful unpack for list based map/reduce
unpackListM :: MRC.UnpackM m x y -> [x] -> m [y]
unpackListM (MRC.FilterM t) = filterM t
unpackListM (MRC.UnpackM f) = fmap L.concat . traverse (fmap F.toList . f)
{-# INLINABLE unpackListM #-}

-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByHashableKey :: (Hashable k, Eq k) => [(k, c)] -> [(k, Seq.Seq c)]
groupByHashableKey =
  HMS.toList . HMS.fromListWith (<>) . fmap (second Seq.singleton)
{-# INLINABLE groupByHashableKey #-}

-- | group the mapped and assigned values by key using a Data.HashMap.Strict
groupByOrderedKey :: Ord k => [(k, c)] -> [(k, Seq.Seq c)]
groupByOrderedKey =
  MS.toList . MS.fromListWith (<>) . fmap (second Seq.singleton)
{-# INLINABLE groupByOrderedKey #-}

-- | map-reduce-fold builder using (Hashable k, Eq k) keys and returning a [] result
listEngine
  :: (Foldable g, Functor g)
  => ([(k, c)] -> [(k, g c)])
  -> MRE.MapReduceFold y k c [] x d
listEngine groupByKey u (MRC.Assign a) r = fmap
  (fmap (uncurry $ MRE.reduceFunction r) . groupByKey . fmap a . unpackList u)
  FL.list
{-# INLINABLE listEngine #-}

-- | effectful map-reduce-fold builder using (Hashable k, Eq k) keys and returning a [] result
listEngineM
  :: (Monad m, Traversable g)
  => ([(k, c)] -> [(k, g c)])
  -> MRE.MapReduceFoldM m y k c [] x d
listEngineM groupByKey u (MRC.AssignM a) rM = MRC.postMapM
  ( (traverse (uncurry $ MRE.reduceFunctionM rM) =<<)
  . fmap groupByKey
  . (traverse a =<<)
  . unpackListM u
  )
  (FL.generalize FL.list)
{-# INLINABLE listEngineM #-}

