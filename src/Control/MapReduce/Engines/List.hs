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
Module      : Control.MapReduce.Engines.List
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using [] as it's intermediate type.
-}
module Control.MapReduce.Engines.List
  (
  -- ** Helpers
    unpackList
  , unpackListM
  -- ** partial builders
  , listEngine
  , listEngineM
  -- ** Engines  
  , lazyHashMapListEngine
  , lazyMapListEngine
  , lazyHashMapListEngineM
  , lazyMapListEngineM
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import qualified Control.Foldl                 as FL
--import           Control.Monad                  ( join )
import           Data.Bool                      ( bool )
import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Lazy             as HML
import qualified Data.Map                      as ML

import           Control.Arrow                  ( second )



-- | case analysis of Unpack for list based mapReduce
unpackList :: MRC.Unpack x y -> [x] -> [y]
unpackList (MRC.Filter t) = L.filter t
unpackList (MRC.Unpack f) = L.concat . fmap (F.toList . f)
{-# INLINABLE unpackList #-}

-- | case analysis of Unpack for list based mapReduce
unpackListM :: MRC.UnpackM m x y -> [x] -> m [y]
unpackListM (MRC.FilterM t) = return . L.filter t
unpackListM (MRC.UnpackM f) = fmap L.concat . traverse (fmap F.toList . f)
{-# INLINABLE unpackListM #-}

-- | map-reduce-fold engine using (Hashable k, Eq k) keys and returning a [] result
lazyHashMapListEngine :: (Hashable k, Eq k) => MRE.MapReduceFold y k c [] x d
lazyHashMapListEngine =
  listEngine (HML.toList . HML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyHashMapListEngine #-}

-- | map-reduce-fold engine using (Ord k) keys and returning a [] result
lazyMapListEngine :: Ord k => MRE.MapReduceFold y k c [] x d
lazyMapListEngine =
  listEngine (ML.toList . ML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyMapListEngine #-}

-- | effectful map-reduce-fold engine using (Hashable k, Eq k) keys and returning a [] result
lazyHashMapListEngineM
  :: (Monad m, Hashable k, Eq k) => MRE.MapReduceFoldM m y k c [] x d
lazyHashMapListEngineM =
  listEngineM (HML.toList . HML.fromListWith (<>) . fmap (second (pure @[])))

-- | effectful map-reduce-fold engine using (Ord k) keys and returning a [] result
lazyMapListEngineM :: (Monad m, Ord k) => MRE.MapReduceFoldM m y k c [] x d
lazyMapListEngineM =
  listEngineM (ML.toList . ML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyHashMapListEngineM #-}

-- | map-reduce-fold engine builder using (Hashable k, Eq k) keys and returning a [] result
listEngine :: ([(k, c)] -> [(k, [c])]) -> MRE.MapReduceFold y k c [] x d
listEngine groupByKey u (MRC.Assign a) r = fmap
  (fmap (uncurry $ MRE.reduceFunction r) . groupByKey . fmap a . unpackList u)
  FL.list
{-# INLINABLE listEngine #-}

-- | effectful map-reduce-fold engine builder using (Hashable k, Eq k) keys and returning a [] result
listEngineM
  :: Monad m => ([(k, c)] -> [(k, [c])]) -> MRE.MapReduceFoldM m y k c [] x d
listEngineM groupByKey u (MRC.AssignM a) rM = MRC.postMapM
  ( (traverse (uncurry $ MRE.reduceFunctionM rM) =<<)
  . (fmap groupByKey)
  . (traverse a =<<)
  . unpackListM u
  )
  (FL.generalize FL.list)
{-# INLINABLE listEngineM #-}

