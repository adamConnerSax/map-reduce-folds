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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

Types and functions used by all the engines.
Notes:

  1. The provided grouping functions group elements into a 'Data.Sequence.Seq' as this is a good default choice.
  2. The <http://hackage.haskell.org/package/streamly Streamly> engine is the fastest in my benchmarks.  It's the engine used by default if you import @Control.MapReduce.Simple@.
  3. All the engines take a grouping function as a parameter and default ones are provided.  For simple map/reduce, the grouping step may be the bottleneck and I wanted to leave room for experimentation.  I've tried (and failed!) to find anything faster than using 'Map' or 'HashMap' via @toList . fromListWith (<>)@.

-}
module Control.MapReduce.Engines
  (
    -- * Fold Types
    MapReduceFold
  , MapReduceFoldM

  -- * Engine Helpers
  , reduceFunction
  , reduceFunctionM

  -- * @groupBy@ Helpers
  , fromListWithHT
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.Foldl                 as FL
import           Control.Monad.ST              as ST
import           Data.Hashable                  ( Hashable )

import qualified Data.HashTable.Class          as HT


-- | Type-alias for a map-reduce-fold engine
type MapReduceFold y k c q x d = MRC.Unpack x y -> MRC.Assign k y c -> MRC.Reduce k c d -> FL.Fold x (q d)

-- | Type-alias for a monadic (effectful) map-reduce-fold engine
type MapReduceFoldM m y k c q x d = MRC.UnpackM m x y -> MRC.AssignM m k y c -> MRC.ReduceM m k c d -> FL.FoldM m x (q d)

-- | Turn @Reduce@ into a function we can apply
reduceFunction :: (Foldable h, Functor h) => MRC.Reduce k x d -> k -> h x -> d
reduceFunction (MRC.Reduce     f) k = f k
reduceFunction (MRC.ReduceFold f) k = FL.fold (f k)
{-# INLINABLE reduceFunction #-}

-- | Turn @ReduceM@ into a function we can apply
reduceFunctionM
  :: (Traversable h, Monad m) => MRC.ReduceM m k x d -> k -> h x -> m d
reduceFunctionM (MRC.ReduceM     f) k = f k
reduceFunctionM (MRC.ReduceFoldM f) k = FL.foldM (f k)
{-# INLINABLE reduceFunctionM #-}

{-
-- copied from Frames
-- which causes overlapping instances. 
instance {-# OVERLAPPABLE #-} Grouping Text where
  grouping = contramap hash grouping
-}

{- | an implementation of @fromListWith@ for mutable hashtables from the <http://hackage.haskell.org/package/hashtables-1.2.3.1 hastables>
package.  Basically a copy @fromList@ from that package using mutate instead of insert to apply the given function if the
was already in the map.  Might not be the ideal implementation.
Notes:

* This function is specific hashtable agnostic so you'll have to supply a specific implementation from the package via TypeApplication
* This function returns the hash-table in the @ST@ monad.  You can fold over it (using @foldM@ from @hashtables@)
and then use @runST@ to get the grouped structure out.
-}
fromListWithHT
  :: forall h k v s
   . (HT.HashTable h, Eq k, Hashable k)
  => (v -> v -> v)
  -> [(k, v)]
  -> ST.ST s (h s k v)
fromListWithHT f l = do
  ht <- HT.new
  go ht l
 where
  g x mx = (Just $ maybe x (`f` x) mx, ())
  go ht = go'
   where
    go' []              = return ht
    go' ((!k, !v) : xs) = do
      HT.mutate ht k (g v)
      go' xs
{-# INLINABLE fromListWithHT #-}

