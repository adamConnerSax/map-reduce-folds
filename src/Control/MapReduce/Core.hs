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
  -- * Gather and sort
  , Gatherer(..)
  -- * Combine into map-reduce folds
  -- ** map/assign/gather strategies
  -- *** non-monadic
  , uagMapAllGatherEachFold
  , uagMapEachFold
  , uagMapAllGatherOnceFold
  -- *** monadic
  , uagMapAllGatherEachFoldM
  , uagMapEachFoldM
  , uagMapAllGatherOnceFoldM
  -- ** map-reduce
  -- *** non-monadic  
  , mapReduceFold
  -- *** monadic
  , mapReduceFoldM
  -- * Utilities
  -- ** functions to generalize non-monadic to monadic
  , generalizeUnpack
  , generalizeAssign
  , generalizeReduce
    -- ** Helper type for constraint specification
  , Empty
  )
where

import qualified Control.Foldl                 as FL
import           Data.Monoid                    ( Monoid(..) )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
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

-- Not a class because for the same map we may want different methods of folding and traversing
-- E.g., for a parallel mapReduce
-- That is also the reason we carry an extra constraint.  We'll need (NFData e) but only for the parallel version

-- | gathers pairs @(k,c)@ into @gt@ (@gt@ could be @[(k,c)]@ or @Data.Sequence.Seq (k,c)@ or @Data.Map k [c]@, etc.)
-- groups them by @k@, merges the @c@'s for each group into a d and then provides methods to use a function
-- @(Monoid e => k -> d -> e)@ to foldMap over the resulting pairs @(k,d)@ into a result @e@.  
-- NB: @d@ could be @[c]@ or, if @c@ has a Semigroup instance, you could have @d ~ c@
-- We carry an extra constraint in case we need to have an NFData constraint for Control.Parallel.Strategies
data Gatherer (eConst :: Type -> Constraint) gt k c d =
  Gatherer
  {
    foldInto :: forall h. Foldable h => h (k,c) -> gt -- ^ gather a foldable of @(k,c)@ pairs
  , gFoldMapWithKey :: forall e. (eConst e, Monoid e) => (k -> d -> e) -> gt -> e -- ^ combine all the @c@s into a @d@, traverse the result and mconcat
  , gFoldMapWithKeyM :: forall e n. (eConst e, Monoid e, Monad n) => (k -> d -> n e) -> gt -> n e -- ^ combine all the @c@s into a @d@, traverse the result and mconcat. Monadically.
  }

-- | represent an empty constraint
class Empty x
instance Empty x

-- | Do the map step by unpacking to a monoid, merge those monoids via mappend, then do the assigning and grouping 
uagMapEachFold
  :: (Monoid (g y), Traversable g)
  => Gatherer ec gt k c d
  -> Unpack g x y
  -> Assign k y c
  -> FL.Fold x gt
uagMapEachFold g u a =
  let (Unpack unpack) = u
      (Assign assign) = a
  in  P.dimap unpack (foldInto g . fmap assign) FL.mconcat
 where
{-# INLINABLE uagMapEachFold #-}

-- | Do the map step by unpacking, assigning such that the unpacked and assigned items are monoidal, merge those via mappend then do the grouping   
uagMapAllGatherOnceFold
  :: (Monoid (g (k, c)), Traversable g)
  => Gatherer ec gt k c d
  -> Unpack g x y
  -> Assign k y c
  -> FL.Fold x gt
uagMapAllGatherOnceFold g u a =
  let (Unpack unpack) = u
      (Assign assign) = a
  in  P.dimap (fmap assign . unpack) (foldInto g) FL.mconcat
{-# INLINABLE uagMapAllGatherOnceFold #-}

-- | Do the map step by unpacking, assigning then gathering to a monoid in each key.  This one is the most general since (g a) need not be a monoid
uagMapAllGatherEachFold
  :: (Monoid gt, Traversable g)
  => Gatherer ec gt k c d
  -> Unpack g x y
  -> Assign k y c
  -> FL.Fold x gt
uagMapAllGatherEachFold g u a =
  let (Unpack unpack) = u
      (Assign assign) = a
  in  FL.premap (foldInto g . fmap assign . unpack) FL.mconcat
{-# INLINABLE uagMapAllGatherEachFold #-}

-- | Do the map step by unpacking to a monoid, merge those monoids via mappend, then do the assigning and grouping 
uagMapEachFoldM
  :: (Monad m, Monoid (g y), Traversable g)
  => Gatherer ec gt k c d
  -> UnpackM m g x y
  -> AssignM m k y c
  -> FL.FoldM m x gt
uagMapEachFoldM g u a =
  let (UnpackM unpackM) = u
      (AssignM assignM) = a
  in  FL.premapM unpackM
      $ postMapM (fmap (foldInto g) . traverse assignM)
      $ FL.generalize FL.mconcat
{-# INLINABLE uagMapEachFoldM #-}

-- | Do the map step by unpacking, assigning such that the unpacked and assigned items are monoidal, merge those via mappend then do the grouping   
uagMapAllGatherOnceFoldM
  :: (Monad m, Monoid (g (k, c)), Traversable g)
  => Gatherer ec gt k c d
  -> UnpackM m g x y
  -> AssignM m k y c
  -> FL.FoldM m x gt
uagMapAllGatherOnceFoldM g u a =
  let (UnpackM unpackM) = u
      (AssignM assignM) = a
  in  FL.premapM ((traverse assignM =<<) . unpackM)
      $ fmap (foldInto g)
      $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherOnceFoldM #-}

-- | Do the map step by unpacking, assigning then gathering to a monoid in each key.  This one is the most general since (g a) need not be a monoid
uagMapAllGatherEachFoldM
  :: (Monad m, Monoid gt, Traversable g)
  => Gatherer ec gt k c d
  -> UnpackM m g x y
  -> AssignM m k y c
  -> FL.FoldM m x gt
uagMapAllGatherEachFoldM g u a =
  let (UnpackM unpackM) = u
      (AssignM assignM) = a
  in  FL.premapM (fmap (foldInto g) . (traverse assignM =<<) . unpackM)
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherEachFoldM #-}
-- NB:  (\f -> (traverse f =<<)) :: (Traversable t, Applicative m) => (a -> m b) -> m (t a) -> m (t b) 
-- that is, (traverse f =<<) = join . fmap (traverse f). Not sure I prefer the former.  But hlint does...

-- | Wrapper for functions to reduce keyed and grouped data to the result type
-- there are four constructors because we handle non-monadic and monadic reductions and
-- we pay special attention to reductions which are themselves folds since they may be combined
-- applicatively with greater efficiency.
data Reduce k h x e where
  Reduce :: (k -> h x -> e) -> Reduce k h x e
  ReduceFold :: Foldable h => (k -> FL.Fold x e) -> Reduce k h x e

data ReduceM m k h x e where
  ReduceM :: Monad m => (k -> h x -> m e) -> ReduceM m k h x e
  ReduceFoldM :: (Monad m, Foldable h) => (k -> FL.FoldM m x e) -> ReduceM m k h x e

instance Functor (Reduce k h x) where
  fmap f (Reduce g) = Reduce $ \k -> f . g k
  fmap f (ReduceFold g) = ReduceFold $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor (ReduceM m k h x) where
  fmap f (ReduceM g) = ReduceM $ \k -> fmap f . g k
  fmap f (ReduceFoldM g) = ReduceFoldM $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor h => P.Profunctor (Reduce k h) where
  dimap l r (Reduce g)  = Reduce $ \k -> P.dimap (fmap l) r (g k)
  dimap l r (ReduceFold g) = ReduceFold $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Functor h => P.Profunctor (ReduceM m k h) where
  dimap l r (ReduceM g)  = ReduceM $ \k -> P.dimap (fmap l) (fmap r) (g k)
  dimap l r (ReduceFoldM g) = ReduceFoldM $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Foldable h => Applicative (Reduce k h x) where
  pure x = ReduceFold $ const (pure x)
  {-# INLINABLE pure #-}
  Reduce r1 <*> Reduce r2 = Reduce $ \k -> r1 k <*> r2 k
  ReduceFold f1 <*> ReduceFold f2 = ReduceFold $ \k -> f1 k <*> f2 k
  Reduce r1 <*> ReduceFold f2 = Reduce $ \k -> r1 k <*> FL.fold (f2 k)
  ReduceFold f1 <*> Reduce r2 = Reduce $ \k -> FL.fold (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (ReduceM m k h x) where
  pure x = ReduceM $ \_ -> pure $ pure x
  {-# INLINABLE pure #-}
  ReduceM r1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> r1 k <*> r2 k
  ReduceFoldM f1 <*> ReduceFoldM f2 = ReduceFoldM $ \k -> f1 k <*> f2 k
  ReduceM r1 <*> ReduceFoldM f2 = ReduceM $ \k -> (<*>) <$> r1 k <*> FL.foldM (f2 k)
  ReduceFoldM f1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> FL.foldM (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

-- | Make a non-monadic reduce monadic.  Used to match types in the final fold when the unpack step is monadic
-- but reduce is not.
generalizeReduce :: Monad m => Reduce k h x e -> ReduceM m k h x e
generalizeReduce (Reduce     f) = ReduceM $ \k -> return . f k
generalizeReduce (ReduceFold f) = ReduceFoldM $ FL.generalize . f
{-# INLINABLE generalizeReduce #-}

-- | put all the steps together with a gatherer to make the map-reduce fold
mapReduceFold
  :: (Monoid e, ec e, Foldable h, Monoid gt, Traversable g)
  => (  Gatherer ec gt k c (h z)
     -> Unpack g x y
     -> Assign k y c
     -> FL.Fold x gt
     )
  -> Gatherer ec gt k c (h z)
  -> Unpack g x y
  -> Assign k y c
  -> Reduce k h z e
  -> FL.Fold x e
mapReduceFold gatherStrategy gatherer unpack assign reduce = fmap
  (gFoldMapWithKey gatherer reducer)
  mapFold
 where
  mapFold = gatherStrategy gatherer unpack assign
  reducer = case reduce of
    Reduce     f -> f
    ReduceFold f -> (\k hx -> FL.fold (f k) hx)
{-# INLINABLE mapReduceFold #-}

-- | put all the (monadic) steps together with a gatherer to make the map-reduce fold
mapReduceFoldM
  :: (Monad m, Monoid e, ec e, Foldable h, Monoid gt, Traversable g)
  => (  Gatherer ec gt k c (h z)
     -> UnpackM m g x y
     -> AssignM m k y c
     -> FL.FoldM m x gt
     )
  -> Gatherer ec gt k c (h z)
  -> UnpackM m g x y
  -> AssignM m k y c
  -> ReduceM m k h z e
  -> FL.FoldM m x e
mapReduceFoldM gatherStrategyM gatherer unpackM assignM reduceM = postMapM
  (gFoldMapWithKeyM gatherer reducerM)
  mapFoldM
 where
  mapFoldM = gatherStrategyM gatherer unpackM assignM
  reducerM = case reduceM of
    ReduceM     f -> f
    ReduceFoldM f -> (\k hx -> FL.foldM (f k) hx)
{-# INLINABLE mapReduceFoldM #-}

-- TODO: submit a PR to foldl for this
-- | Helper for the traversal step in monadic folds
postMapM :: Monad m => (a -> m b) -> FL.FoldM m x a -> FL.FoldM m x b
postMapM f (FL.FoldM step begin done) = FL.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE postMapM #-}
