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
module Control.MapReduce.Engines where

import qualified Control.MapReduce.Core        as MRC

import qualified Control.Foldl                 as FL

import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Lazy             as HML
import           Data.Monoid                    ( Monoid(..) )

--import qualified Data.Profunctor               as P
import           Control.Arrow                  ( second )
import           GHC.Exts                       ( augment )

data Reduce k x d where
  Reduce :: (forall h. Foldable h => k -> (h x -> d)) -> Reduce k x d
  ReduceFold :: (k -> FL.Fold x d) -> Reduce k x d

reduceFunction :: Foldable h => Reduce k x d -> k -> h x -> d
reduceFunction (Reduce     f) = f
reduceFunction (ReduceFold f) = \k -> FL.fold (f k)
{-# INLINABLE reduceFunction #-}

type MapReduceFold g y k c q x d = MRC.Unpack g x y -> MRC.Assign k y c -> Reduce k c d -> FL.Fold x (q d)

lazyHashMapListEngine
  :: (Functor g, Foldable g, Hashable k, Eq k) => MapReduceFold g y k c [] x d
lazyHashMapListEngine =
  listEngine (HML.toList . HML.fromListWith (<>) . fmap (second (pure @[])))
{-# INLINABLE lazyHashMapListEngine #-}

listEngine
  :: (Functor g, Foldable g)
  => ([(k, c)] -> [(k, [c])])
  -> MapReduceFold g y k c [] x d
listEngine groupByKey (MRC.Unpack u) (MRC.Assign a) r = fmap
  (fmap (uncurry $ reduceFunction r) . groupByKey . mconcat . fmap
    (F.toList . fmap a . u)
  )
  FL.list
{-# INLINABLE listEngine #-}

{-
listEngine2
  :: (Functor g, Foldable g, Monoid e, Hashable k, Eq k)
  => MapReduceEngine g y k c x [(k,d)]
listEngine2 (MRC.Unpack u) (MRC.Assign a) r =
  let toMap = HML.fromListWith (<>)
      mapWithKey f = F.fold . HML.mapWithKey f   --HML.foldlWithKey' (\e k d -> e <> f k d) mempty
  in  fmap
        ( mapWithKey (reduceFunction r)
        . toMap
        . fmap (second (pure @[]))
        . mconcat
        . fmap (F.toList . fmap a . u)
        )
        FL.list
{-# INLINABLE listEngine2 #-}
-}
