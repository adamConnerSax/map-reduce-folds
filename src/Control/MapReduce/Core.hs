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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Core
Description : a map-reduce wrapper around foldl 
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

MapReduce as folds
This is all just wrapping around Control.Foldl so that it's easier to see the map-reduce structure
The Mapping step is broken into 3. parts:

1. unpacking, which could include "melting" or filtering,

2. assigning, which assigns a group to each unpacked item.  Could just be choosing a key column(s)

3. gathering, which pulls together the items in each group

The reduce step is conceptually simpler, just requiring a function from the (key, grouped data) pair to the result monoid.

But note that reduce could be as simple as combining the key with a single data row or some very complex function of the grouped data.
E.g., reduce could itself be a map-reduce on the grouped data.
Since these are folds, we can share work by using the Applicative instance of MapStep (just the Applicative instance of Control.Foldl.Fold)
and we will loop over the data only once.
The Reduce type is also Applicative so there could be work sharing there as well:
e.g., if your `reduce :: (k -> d -> e)` has the form `reduce k :: FL.Fold d e`

A couple of parts are less straightforward.  The "Gatherer" (a record-of-functions for gathering, grouping and traversing over the groups)
is responsible for choosing what structure holds the grouped data.  The default choices are map or hash map.  Lazy and strict variants seem to
have similar performance in benchmarks so lazy is likely a better choice since then you can perhaps avoid doing work on grouped data you don' use.

And there is the question of how data is grouped for each key after assigning. The simplest choice
is as a list but other options are possible.  This choice is typically made via a function from the data part of the assign
to a monoid.  So if assign chooses a (k, c) pair for each unpacked datum, we group to (k, d) where d is a monoid.  And
we specify this via a choice of `Monoid d => (c->d)`, often, for gathering into lists, just `pure @[] :: c -> [c]`

Several types have a parameter, @mm@, which is a type-level @Maybe@ used to indicate non-monadic @(mm ~ Nothing)@ vs monadic @(Monad m, mm ~ ('Just m))@.
This allows for handling non-monadic and monadic folds with the same functions.  But unpacking and reducing must either both be monadic or neither so there
are included functions for generalizing non-monadic Unpack/Reduce to monadic when required.
-}
module Control.MapReduce.Core
  (
    -- * Basic Types for map reduce
    -- ** non-monadic
    Unpack(..)
  , Assign(..)
  , Reduce(..)
  -- ** monadic
  , UnpackM(..)
  , AssignM(..)
  , ReduceM(..)
  -- ** functions to generalize non-monadic to monadic
  , generalizeUnpack
  , generalizeAssign
  , generalizeReduce
  -- * Foldl helpers
  , postMapM
  -- * re-exports
  , Fold
  , FoldM
  )
where

import qualified Control.Foldl                 as FL
import           Control.Foldl                  ( Fold
                                                , FoldM
                                                ) -- for re-exporting

import qualified Data.Profunctor               as P
import           Control.Arrow                  ( second )

-- | `Unpack` is for "melting" rows (@g ~ [])@ or filtering items (@g ~ Maybe@).
data Unpack g x y where
  Unpack :: (x -> g y) -> Unpack g x y

instance Functor g => Functor (Unpack g x) where
  fmap h (Unpack f) = Unpack (fmap h . f)
  {-# INLINABLE fmap #-}

instance Functor g => P.Profunctor (Unpack g) where
  dimap l r (Unpack f) = Unpack ( fmap r . f . l)
  {-# INLINABLE dimap #-}

-- | `UnpackM` is for "melting" rows (@g ~ [])@ or filtering items (@g ~ Maybe@). This version has a monadic result type to
-- accomodate unpacking that might require randomness or logging during unpacking.
data UnpackM m g x y where
  UnpackM :: Monad m => (x -> m (g y)) -> UnpackM m g x y

instance Functor g => Functor (UnpackM m g x) where
  fmap h (UnpackM f) = UnpackM (fmap (fmap h) . f)
  {-# INLINABLE fmap #-}

instance Functor g => P.Profunctor (UnpackM m g) where
  dimap l r (UnpackM f) = UnpackM ( fmap (fmap r) . f . l)
  {-# INLINABLE dimap #-}

-- | "lift" a non-monadic Unpack to a monadic one for any monad m
generalizeUnpack :: Monad m => Unpack g x y -> UnpackM m g x y
generalizeUnpack (Unpack f) = UnpackM $ return . f
{-# INLINABLE generalizeUnpack #-}

-- | Associate a key with a given item/row
data Assign k y c where
  Assign :: (y -> (k, c)) -> Assign k y c

instance Functor (Assign k y) where
  fmap f (Assign h) = Assign $ second f . h --(\y -> let (k,c) = g y in (k, f c))
  {-# INLINABLE fmap #-}

instance P.Profunctor (Assign k) where
  dimap l r (Assign h) = Assign $ second r . h . l --(\z -> let (k,c) = g (l z) in (k, r c))
  {-# INLINABLE dimap #-}

-- | Associate a key with a given item/row.  Monadic return type might be required for DB lookup of keys or logging during assigning.
data AssignM m k y c where
  AssignM :: Monad m => (y -> m (k, c)) -> AssignM m k y c

instance Functor (AssignM m k y) where
  fmap f (AssignM h) = AssignM $ fmap (second f) . h
  {-# INLINABLE fmap #-}

instance P.Profunctor (AssignM m k) where
  dimap l r (AssignM h) = AssignM $ fmap (second r) . h . l
  {-# INLINABLE dimap #-}


-- | "lift" a non-monadic Assign to a monadic one for any monad m
generalizeAssign :: Monad m => Assign k y c -> AssignM m k y c
generalizeAssign (Assign h) = AssignM $ return . h
{-# INLINABLE generalizeAssign #-}

-- | Wrapper for functions to reduce keyed and grouped data to the result type
-- there are four constructors because we handle non-monadic and monadic reductions and
-- we pay special attention to reductions which are themselves folds since they may be combined
-- applicatively with greater efficiency.
data Reduce k x d where
  Reduce :: (k -> (forall h. (Foldable h, Functor h) => h x -> d)) -> Reduce k x d
  ReduceFold :: (k -> FL.Fold x d) -> Reduce k x d

data ReduceM m k x d where
  ReduceM :: Monad m => (k -> (forall h. (Foldable h, Functor h) => h x -> m d)) -> ReduceM m k x d
  ReduceFoldM :: Monad m => (k -> FL.FoldM m x d) -> ReduceM m k x d

instance Functor (Reduce k x) where
  fmap f (Reduce g) = Reduce $ \k -> f . g k
  fmap f (ReduceFold g) = ReduceFold $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor (ReduceM m k x) where
  fmap f (ReduceM g) = ReduceM $ \k -> fmap f . g k
  fmap f (ReduceFoldM g) = ReduceFoldM $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance P.Profunctor (Reduce k) where
  dimap l r (Reduce g)  = Reduce $ \k -> P.dimap (fmap l) r (g k)
  dimap l r (ReduceFold g) = ReduceFold $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance P.Profunctor (ReduceM m k) where
  dimap l r (ReduceM g)  = ReduceM $ \k -> P.dimap (fmap l) (fmap r) (g k)
  dimap l r (ReduceFoldM g) = ReduceFoldM $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Applicative (Reduce k x) where
  pure x = ReduceFold $ const (pure x)
  {-# INLINABLE pure #-}
  Reduce r1 <*> Reduce r2 = Reduce $ \k -> r1 k <*> r2 k
  ReduceFold f1 <*> ReduceFold f2 = ReduceFold $ \k -> f1 k <*> f2 k
  Reduce r1 <*> ReduceFold f2 = Reduce $ \k -> r1 k <*> FL.fold (f2 k)
  ReduceFold f1 <*> Reduce r2 = Reduce $ \k -> FL.fold (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (ReduceM m k x) where
  pure x = ReduceM $ \_ -> pure $ pure x
  {-# INLINABLE pure #-}
  ReduceM r1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> r1 k <*> r2 k
  ReduceFoldM f1 <*> ReduceFoldM f2 = ReduceFoldM $ \k -> f1 k <*> f2 k
  ReduceM r1 <*> ReduceFoldM f2 = ReduceM $ \k -> (<*>) <$> r1 k <*> FL.foldM (f2 k)
  ReduceFoldM f1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> FL.foldM (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

-- | Make a non-monadic reduce monadic.  Used to match types in the final fold when the unpack step is monadic
-- but reduce is not.
generalizeReduce :: Monad m => Reduce k x d -> ReduceM m k x d
generalizeReduce (Reduce     f) = ReduceM $ \k -> return . f k
generalizeReduce (ReduceFold f) = ReduceFoldM $ \k -> FL.generalize (f k)
{-# INLINABLE generalizeReduce #-}


-- TODO: submit a PR to foldl for this
-- | Helper for the traversal step in monadic folds
postMapM :: Monad m => (a -> m b) -> FL.FoldM m x a -> FL.FoldM m x b
postMapM f (FL.FoldM step begin done) = FL.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE postMapM #-}
