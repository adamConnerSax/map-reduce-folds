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
The Mapping step is broken into 2 parts:

1. unpacking, which could include "melting" or filtering,

2. assigning, which assigns a group to each unpacked item.  Could just be choosing a key column(s)

The items are then grouped by key and "reduced"

The reduce step is conceptually simpler, just requiring a function from the (key, grouped data) pair to the result.

Reduce could be as simple as combining the key with a single data row or some very complex function of the grouped data.
E.g., reduce could itself be a map-reduce on the grouped data.
Since these are folds, we can share work by using the Applicative instance of MapStep (just the Applicative instance of Control.Foldl.Fold)
and we will loop over the data only once.
The Reduce type is also Applicative so there could be work sharing there as well, especially if you
specify your reduce as a Fold.
e.g., if your @reduce :: (k -> h c -> d)@ has the form @reduce :: k -> FL.Fold c d@

We combine these steps with an "Engine", resulting in a fold from a container of @x@ to some container of @d@.
The Engine amounts to a choice of grouping algorithm (usually using @Data.Map@ or @Data.HashMap@) and a choice of result container type.
The result container type is used for the intermediate steps as well.

The goal is to make assembling a large family of common map/reduce patterns in a straightforward way.  At some level of complication, you may as
well write them by hand.  An in-between case would be writing the unpack function as a complex hand written filter
-}
module Control.MapReduce.Core
  (
    -- * Basic Types for map reduce
    -- ** Non-Monadic
    Unpack(..)
  , Assign(..)
  , Reduce(..)

  -- ** Monadic
  , UnpackM(..)
  , AssignM(..)
  , ReduceM(..)

  -- ** Non-Monadic -> Monadic
  , generalizeUnpack
  , generalizeAssign
  , generalizeReduce

  -- * Foldl helpers  
  , functionToFold
  , functionToFoldM
  , postMapM

  -- * Re-Exports
  , Fold
  , FoldM
  , fold
  , foldM
  )
where

import qualified Control.Foldl                 as FL
import           Control.Foldl                  ( Fold
                                                , FoldM
                                                , fold
                                                , foldM
                                                ) -- for re-exporting

import qualified Data.Profunctor               as P
import           Data.Profunctor                ( Profunctor )
import qualified Data.Sequence                 as S
import           Control.Arrow                  ( second )

-- | @Unpack@ is for filtering rows or "melting" them, that is doing something which turns each @x@ into several @y@.
-- Filter is a special case because it can often be done faster directly than via folding over @Maybe@
data Unpack x y where
  Filter :: (x -> Bool) -> Unpack x x -- we single out this special case because it's faster to do directly
  Unpack :: Traversable g => (x -> g y) -> Unpack x y -- we only need (Functor g, Foldable g) but if we want to generalize we need Traversable

-- helpers for turning filter functions into @a -> Maybe a@
boolToMaybe :: Bool -> a -> Maybe a
boolToMaybe b x = if b then Just x else Nothing

ifToMaybe :: (x -> Bool) -> x -> Maybe x
ifToMaybe t x = boolToMaybe (t x) x

instance Functor (Unpack x) where
  fmap h (Filter t) = Unpack (fmap h . ifToMaybe t)
  fmap h (Unpack f) = Unpack (fmap h . f)
  {-# INLINABLE fmap #-}

instance P.Profunctor Unpack where
  dimap l r (Filter t) = Unpack ( fmap r . ifToMaybe t . l)
  dimap l r (Unpack f) = Unpack ( fmap r . f . l)
  {-# INLINABLE dimap #-}

-- | @UnpackM@ is for filtering or melting the input type. This version has a monadic result type to
-- accomodate unpacking that might require randomness or logging during unpacking.
-- filter is a special case since filtering can be often be done faster.  So we single it out. 
data UnpackM m x y where
  FilterM :: Monad m => (x -> m Bool) -> UnpackM m x x
  UnpackM :: (Monad m, Traversable g) => (x -> m (g y)) -> UnpackM m x y

ifToMaybeM :: Monad m => (x -> m Bool) -> x -> m (Maybe x)
ifToMaybeM t x = fmap (`boolToMaybe` x) (t x)

instance Functor (UnpackM m x) where
  fmap h (FilterM t) = UnpackM (fmap (fmap h) . ifToMaybeM t)
  fmap h (UnpackM f) = UnpackM (fmap (fmap h) . f)
  {-# INLINABLE fmap #-}

instance Profunctor (UnpackM m) where
  dimap l r (FilterM t) = UnpackM ( fmap (fmap r) . ifToMaybeM t . l)
  dimap l r (UnpackM f) = UnpackM ( fmap (fmap r) . f . l)
  {-# INLINABLE dimap #-}

-- | lift a non-monadic Unpack to a monadic one for any monad m
generalizeUnpack :: Monad m => Unpack x y -> UnpackM m x y
generalizeUnpack (Filter t) = FilterM $ return . t
generalizeUnpack (Unpack f) = UnpackM $ return . f
{-# INLINABLE generalizeUnpack #-}

-- | map @y@ into a @(k,c)@ pair for grouping 
data Assign k y c where
  Assign :: (y -> (k, c)) -> Assign k y c

instance Functor (Assign k y) where
  fmap f (Assign h) = Assign $ second f . h --(\y -> let (k,c) = g y in (k, f c))
  {-# INLINABLE fmap #-}

instance Profunctor (Assign k) where
  dimap l r (Assign h) = Assign $ second r . h . l --(\z -> let (k,c) = g (l z) in (k, r c))
  {-# INLINABLE dimap #-}

-- | Effectfully map @y@ into a @(k,c)@ pair for grouping 
data AssignM m k y c where
  AssignM :: Monad m => (y -> m (k, c)) -> AssignM m k y c

instance Functor (AssignM m k y) where
  fmap f (AssignM h) = AssignM $ fmap (second f) . h
  {-# INLINABLE fmap #-}

instance Profunctor (AssignM m k) where
  dimap l r (AssignM h) = AssignM $ fmap (second r) . h . l
  {-# INLINABLE dimap #-}


-- | lift a non-monadic Assign to a monadic one for any monad m
generalizeAssign :: Monad m => Assign k y c -> AssignM m k y c
generalizeAssign (Assign h) = AssignM $ return . h
{-# INLINABLE generalizeAssign #-}

-- | Wrapper for functions to reduce keyed and grouped data to the result type.
-- It is *strongly* suggested that you use Folds for this step.  Type-inference and
-- applicative optimization are more straightforward that way.  The non-Fold contructors are
-- there in order to retro-fit existing functions.

-- | Reduce step for non-effectful reductions
data Reduce k x d where
  Reduce :: (k -> (forall h. (Foldable h, Functor h) => (h x -> d))) -> Reduce k x d
  ReduceFold :: (k -> FL.Fold x d) -> Reduce k x d

-- | Reduce step for effectful reductions.
-- It is *strongly* suggested that you use Folds for this step.  Type-inference and
-- applicative optimization are more straightforward that way.  The non-Fold contructors are
-- there in order to retro-fit existing functions.
data ReduceM m k x d where
  ReduceM :: Monad m => (k -> (forall h. (Foldable h, Functor h) => (h x -> m d))) -> ReduceM m k x d
  ReduceFoldM :: Monad m => (k -> FL.FoldM m x d) -> ReduceM m k x d

instance Functor (Reduce k x) where
  fmap f (Reduce g) = Reduce $ \k -> f . g k
  fmap f (ReduceFold g) = ReduceFold $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor (ReduceM m k x) where
  fmap f (ReduceM g) = ReduceM $ \k -> fmap f . g k
  fmap f (ReduceFoldM g) = ReduceFoldM $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Profunctor (Reduce k) where
  dimap l r (Reduce g)  = Reduce $ \k -> P.dimap (fmap l) r (g k)
  dimap l r (ReduceFold g) = ReduceFold $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Profunctor (ReduceM m k) where
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

-- | Make a non-monadic reduce monadic. 
generalizeReduce :: Monad m => Reduce k x d -> ReduceM m k x d
generalizeReduce (Reduce     f) = ReduceM $ \k -> return . f k
generalizeReduce (ReduceFold f) = ReduceFoldM $ \k -> FL.generalize (f k)
{-# INLINABLE generalizeReduce #-}

-- TODO: submit a PR to foldl for this
-- | Given an effectful (monadic) Control.Foldl fold, a @FoldM m x a@, we can use its @Functor@ instance
-- to apply a non-effectful @(a -> b)@ to its result type.  To apply @(a -> m b)@, we need this combinator.
postMapM :: Monad m => (a -> m b) -> FL.FoldM m x a -> FL.FoldM m x b
postMapM f (FL.FoldM step begin done) = FL.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE postMapM #-}

seqF :: FL.Fold a (S.Seq a)
seqF = FL.Fold (S.|>) S.empty id

-- | convert a function which takes a foldable container of x and produces an a into a Fold x a.
-- uses Data.Sequence.Seq as an intermediate type because it should behave well whether
-- f is a left or right fold. 
-- This can be helpful in putting an existing function into a Reduce.
functionToFold :: (forall h . Foldable h => h x -> a) -> FL.Fold x a
functionToFold f = fmap f seqF

-- | convert a function which takes a foldable container of x and produces an (m a) into a FoldM m x a.
-- uses Data.Sequence.Seq as an intermediate type because it should behave well whether
-- f is a left or right fold.
-- This can be helpful in putting an existing function into a ReduceM.
functionToFoldM
  :: Monad m => (forall h . Foldable h => h x -> m a) -> FL.FoldM m x a
functionToFoldM f = postMapM f $ FL.generalize seqF

