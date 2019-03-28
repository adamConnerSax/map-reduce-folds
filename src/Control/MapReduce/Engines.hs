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
Module      : Control.MapReduce.Engines
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Control.MapReduce.Engines
  (
    -- * Types
    MapReduceFold
  , MapReduceFoldM
  -- * helpers
  , reduceFunction
  , reduceFunctionM
  )
where

import qualified Control.MapReduce.Core        as MRC

import qualified Control.Foldl                 as FL
--import qualified Data.List                     as L
import qualified Data.Foldable                 as F
--import           Data.Hashable                  ( Hashable )
--import qualified Data.HashMap.Lazy             as HML
--import qualified Data.Map                      as ML

--import           Control.Arrow                  ( second )


-- | case analysis of Reduce
reduceFunction
  :: MRC.Reduce k x d -> k -> (forall h . (Foldable h, Functor h) => h x -> d)
reduceFunction (MRC.Reduce     f) = f
reduceFunction (MRC.ReduceFold f) = \k -> FL.fold (f k)
{-# INLINABLE reduceFunction #-}

-- | case analysis of ReduceM
reduceFunctionM
  :: Monad m
  => MRC.ReduceM m k x d
  -> k
  -> (forall h . (Foldable h, Functor h) => h x -> m d)
reduceFunctionM (MRC.ReduceM     f) = f
reduceFunctionM (MRC.ReduceFoldM f) = \k -> FL.foldM (f k)
{-# INLINABLE reduceFunctionM #-}


-- | describe the signature of a map-reduce-fold-engine
type MapReduceFold y k c q x d = MRC.Unpack x y -> MRC.Assign k y c -> MRC.Reduce k c d -> FL.Fold x (q d)

-- | describe the signature of a monadic (effectful) map-reduce-fold-engine
type MapReduceFoldM m y k c q x d = MRC.UnpackM m x y -> MRC.AssignM m k y c -> MRC.ReduceM m k c d -> FL.FoldM m x (q d)

