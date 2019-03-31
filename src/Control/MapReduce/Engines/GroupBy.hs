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
Module      : Control.MapReduce.Engines.GroupBy
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

use recursion schemes to build group-by functions, that is, functions of the form,
(Ord k, Applicative f, Functor f) => f (k,c) -> f (k,f c)

This is a warm-up for building the entire engine via recursion-schemes
-}
module Control.MapReduce.Engines.GroupBy
  (
    -- * List grouping
    groupByTVL
  )
where

import qualified Data.List                     as L
import           Data.Functor.Foldable         as F
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import           Control.Arrow                  ( second )
{-
data TreeF a r where
  Nil :: TreeF a r
  Leaf :: a -> TreeF a r
  Node :: r -> r -> TreeF a r deriving (Show, Functor)
-}

-- from <https://twanvl.nl/blog/haskell/generic-merge>

-- list merge, preserving ordering of keys and using semigroup (<>) when keys are equal
groupByTVL :: Ord k => [(k,v)] -> [(k,[v])]
groupByTVL = mergeSortUnion . fmap (second $ pure @[])
{-# INLINABLE groupByTVL #-}

mergeSemi :: (Ord k, Semigroup w) => [(k,w)] -> [(k,w)] -> [(k,w)]
mergeSemi = unionByWith (\a b -> compare (fst a) (fst b)) (\(k,w1) (_,w2) -> (k,w1 <> w2))
{-# INLINABLE mergeSemi #-}

unionByWith :: (a -> a -> Ordering) -> (a -> a -> a) -> [a] -> [a] -> [a]
unionByWith cmp f = mergeByR cmp (\a b c -> f a b:c) (:) (:) []
{-# INLINABLE unionByWith #-}

split :: [a] -> ([a],[a])
split (x:y:zs) = let (xs,ys) = split zs in (x:xs,y:ys)
split xs       = (xs,[])
{-# INLINABLE split #-}

mergeSortUnion :: Ord k => [(k,[v])] -> [(k,[v])]
mergeSortUnion [] = []
mergeSortUnion [x] = [x]
mergeSortUnion xs  = let (ys,zs) = split xs in mergeSemi (mergeSortUnion ys) (mergeSortUnion zs)
{-# INLINABLE mergeSortUnion #-}

mergeByR :: (a -> b -> Ordering)  -- ^ cmp: Comparison function
         -> (a -> b -> c -> c)    -- ^ fxy: Combine when a and b are equal
         -> (a -> c -> c)         -- ^ fx:  Combine when a is less
         -> (b -> c -> c)         -- ^ fy:  Combine when b is less
         -> c                     -- ^ z:   Base case
         -> [a] -> [b] -> c       -- ^ Argument lists and result list
mergeByR cmp fxy fx fy z = go
    where go []     ys     = foldr fy z ys
          go xs     []     = foldr fx z xs
          go (x:xs) (y:ys) = case cmp x y of
              LT -> fx  x   (go xs (y:ys))
              EQ -> fxy x y (go xs ys)
              GT -> fy    y (go (x:xs) ys)         
{-# INLINABLE mergeByR #-}


