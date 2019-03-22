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
{-# LANGUAGE InstanceSigs          #-}
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
    Unpack(..)
  , Assign(..)
  , Gatherer(..)
  , Reduce(..)
  , MapFoldT
  -- * Auxiliary types for intermediate steps 
  , MapStep(..)
  , MapGather(..)
  -- * Helper types for constraint specification
  , Empty
  -- * functions to combine unpacking, assigning and gathering
  , uagMapAllGatherEachFold
  , uagMapEachFold
  , uagMapAllGatherEachFold
  -- * functions which assemble all the pieces into a 'Fold'
  , mapReduceFold
  , mapGatherReduceFold
  -- * Managing non-monadic and monadic folds
  -- ** functions to generalize non-monadic to monadic
  , generalizeUnpack
  , generalizeMapStep
  , generalizeReduce
  -- **
  , mapFold
  , IdReducer(..)
  )
where

import qualified Control.Foldl                 as FL
import           Data.Monoid                    ( Monoid(..) )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
import qualified Data.Profunctor               as P



-- | `Unpack` is for "melting" rows (@g ~ [])@ or filtering items (@g ~ Maybe@).
data Unpack (mm :: Maybe (Type -> Type)) g x y where
  Unpack :: (x -> g y) -> Unpack 'Nothing g x y
  UnpackM :: Monad m => (x -> m (g y)) -> Unpack ('Just m) g x y

instance Functor g => Functor (Unpack mm g x) where
  fmap h (Unpack f) = Unpack (fmap h . f)
  fmap h (UnpackM f ) = UnpackM (fmap (fmap h) . f)
  {-# INLINABLE fmap #-}

instance Functor g => P.Profunctor (Unpack mm g) where
  dimap l r (Unpack f) = Unpack ( fmap r . f . l)
  dimap l r (UnpackM f) = UnpackM ( fmap (fmap r) . f . l)
  {-# INLINABLE dimap #-}

generalizeUnpack :: Monad m => Unpack 'Nothing g x y -> Unpack ( 'Just m) g x y
generalizeUnpack (Unpack f) = UnpackM $ return . f
{-# INLINABLE generalizeUnpack #-}

-- | Associate a key with a given item/row
data Assign k y c where
  Assign :: (y -> (k, c)) -> Assign k y c

instance Functor (Assign k y) where
  fmap f (Assign g) = Assign (\y -> let (k,c) = g y in (k, f c))
  {-# INLINABLE fmap #-}

instance P.Profunctor (Assign k) where
  dimap l r (Assign g) = Assign (\z -> let (k,c) = g (l z) in (k, r c))
  {-# INLINABLE dimap #-}

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

-- | `MapStep` is a combination of Unpack, Assign and Gather
-- they can be combined various ways and which one is best depends on the
-- relative complexity of the various steps and the constraints satisfied by
-- the intermediate containers.  
data MapStep (a :: Maybe (Type -> Type)) x q  where -- q ~ f k d
  MapStepFold :: FL.Fold x q -> MapStep 'Nothing x q
  MapStepFoldM :: Monad m => FL.FoldM m x q -> MapStep ('Just m) x q

-- | Generalize a MapStepFold from non-monadic to monadic
generalizeMapStep :: Monad m => MapStep 'Nothing x q -> MapStep ( 'Just m) x q
generalizeMapStep (MapStepFold f) = MapStepFoldM $ FL.generalize f
{-# INLINABLE generalizeMapStep #-}

instance Functor (MapStep mm x) where
  fmap h (MapStepFold fld) = MapStepFold $ fmap h fld
  fmap h (MapStepFoldM fld) = MapStepFoldM $ fmap h fld
  {-# INLINABLE fmap #-}

instance P.Profunctor (MapStep mm) where
  dimap l r (MapStepFold fld) = MapStepFold $ P.dimap l r fld
  dimap l r (MapStepFoldM fld) = MapStepFoldM $ P.dimap l r fld
  {-# INLINABLE dimap #-}

-- NB: we can only share the fold over h x if both inputs are folds
instance Applicative (MapStep 'Nothing x) where
  pure y = MapStepFold $ pure y
  {-# INLINABLE pure #-}
  MapStepFold x <*> MapStepFold y = MapStepFold $ x <*> y
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (MapStep ('Just m) x) where
  pure y = MapStepFoldM $ pure y
  {-# INLINABLE pure #-}
  MapStepFoldM x <*> MapStepFoldM y = MapStepFoldM $ x <*> y
  {-# INLINABLE (<*>) #-}

-- we will export this to reserve the possibility of MapStep being something else internally
-- | type level mapping from @(Maybe (Type -> Type))@ to a fold type
type family MapFoldT (mm :: Maybe (Type -> Type)) :: (Type -> Type -> Type) where
  MapFoldT 'Nothing = FL.Fold
  MapFoldT ('Just m) = FL.FoldM m

-- | type level mapping from @Maybe (Type -> Type)@ to a result type for functions
type family WrapMaybe (mm :: Maybe (Type -> Type)) (a :: Type) :: Type where
  WrapMaybe 'Nothing a = a
  WrapMaybe ('Just m) a = m a

-- | extract the fold from a MapStep
mapFold :: MapStep mm x q -> MapFoldT mm x q
mapFold (MapStepFold  f) = f
mapFold (MapStepFoldM f) = f
{-# INLINABLE mapFold #-}

-- | Type for holding a step and gatherer 
data MapGather mm x ec gt k c d = MapGather { gatherer :: Gatherer ec gt k c d, mapStep :: MapStep mm x gt }

-- Fundamentally 3 ways to combine these operations to produce a MapStep:
-- group . fmap . <> . fmap : "MapEach "
-- group . <> . fmap . fmap : "MapAllGroupOnce" 
--  <> . group . fmap . fmap : "MapAllGroupEach"
-- | Do the map step by unpacking to a monoid, merge those monoids via mappend, then do the assigning and grouping 
uagMapEachFold
  :: (Monoid (g y), Functor g, Foldable g)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d
uagMapEachFold gatherer' unpacker (Assign assign') = MapGather gatherer'
                                                               mapStep'
 where
  mapStep' = case unpacker of
    Unpack unpack -> MapStepFold
      $ P.dimap unpack (foldInto gatherer' . fmap assign') FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM unpackM
        $ fmap (foldInto gatherer' . fmap assign')
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapEachFold #-}

-- | Do the map step by unpacking and assigning to a monoid, merge those monoids via mappend, then do the grouping 
uagMapAllGatherOnceFold
  :: (Monoid (g (k, c)), Functor g, Foldable g)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d --MapStep mm x (mt k d)
uagMapAllGatherOnceFold gatherer' unpacker (Assign assign') = MapGather
  gatherer'
  mapStep'
 where
  mapStep' = case unpacker of
    Unpack unpack -> MapStepFold
      $ P.dimap (fmap assign' . unpack) (foldInto gatherer') FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM (fmap (fmap assign') . unpackM)
        $ fmap (foldInto gatherer')
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherOnceFold #-}

-- | Do the map step by unpacking, then assigning, gathering into a monoid, then grouping.
-- This is the most general since it doesn't require that unpack or assign produce monoidal results.
uagMapAllGatherEachFold
  :: (Functor g, Foldable g, Monoid gt)
  => Gatherer ec gt k c d
  -> Unpack mm g x y
  -> Assign k y c
  -> MapGather mm x ec gt k c d --MapStep mm x (mt k d)
uagMapAllGatherEachFold gatherer' unpacker (Assign assign') = MapGather
  gatherer'
  mapStep'
 where
  mapStep' = case unpacker of
    Unpack unpack -> MapStepFold
      $ FL.premap (foldInto gatherer' . fmap assign' . unpack) FL.mconcat
    UnpackM unpackM ->
      MapStepFoldM
        $ FL.premapM (fmap (foldInto gatherer' . fmap assign') . unpackM)
        $ FL.generalize FL.mconcat
{-# INLINABLE uagMapAllGatherEachFold #-}

-- | Wrapper for functions to reduce keyed and grouped data to the result type
-- there are four constructors because we handle non-monadic and monadic reductions and
-- we pay special attention to reductions which are themselves folds since they may be combined
-- applicatively with greater efficiency.
data Reduce (mm :: Maybe (Type -> Type)) k h x e where
  Reduce :: (k -> h x -> e) -> Reduce 'Nothing k h x e
  ReduceFold :: Foldable h => (k -> FL.Fold x e) -> Reduce 'Nothing k h x e
  ReduceM :: Monad m => (k -> h x -> m e) -> Reduce ('Just m) k h x e
  ReduceFoldM :: (Monad m, Foldable h) => (k -> FL.FoldM m x e) -> Reduce ('Just m) k h x e

instance Functor (Reduce mm k h x) where
  fmap f (Reduce g) = Reduce $ \k -> f . g k
  fmap f (ReduceFold g) = ReduceFold $ \k -> fmap f (g k)
  fmap f (ReduceM g) = ReduceM $ \k -> fmap f . g k
  fmap f (ReduceFoldM g) = ReduceFoldM $ \k -> fmap f (g k)
  {-# INLINABLE fmap #-}

instance Functor h => P.Profunctor (Reduce mm k h) where
  dimap l r (Reduce g)  = Reduce $ \k -> P.dimap (fmap l) r (g k)
  dimap l r (ReduceFold g) = ReduceFold $ \k -> P.dimap l r (g k)
  dimap l r (ReduceM g)  = ReduceM $ \k -> P.dimap (fmap l) (fmap r) (g k)
  dimap l r (ReduceFoldM g) = ReduceFoldM $ \k -> P.dimap l r (g k)
  {-# INLINABLE dimap #-}

instance Foldable h => Applicative (Reduce 'Nothing k h x) where
  pure x = ReduceFold $ const (pure x)
  {-# INLINABLE pure #-}
  Reduce r1 <*> Reduce r2 = Reduce $ \k -> r1 k <*> r2 k
  ReduceFold f1 <*> ReduceFold f2 = ReduceFold $ \k -> f1 k <*> f2 k
  Reduce r1 <*> ReduceFold f2 = Reduce $ \k -> r1 k <*> FL.fold (f2 k)
  ReduceFold f1 <*> Reduce r2 = Reduce $ \k -> FL.fold (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

instance Monad m => Applicative (Reduce ('Just m) k h x) where
  pure x = ReduceM $ \_ -> pure $ pure x
  {-# INLINABLE pure #-}
  ReduceM r1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> r1 k <*> r2 k
  ReduceFoldM f1 <*> ReduceFoldM f2 = ReduceFoldM $ \k -> f1 k <*> f2 k
  ReduceM r1 <*> ReduceFoldM f2 = ReduceM $ \k -> (<*>) <$> r1 k <*> FL.foldM (f2 k)
  ReduceFoldM f1 <*> ReduceM r2 = ReduceM $ \k -> (<*>) <$> FL.foldM (f1 k) <*> r2 k
  {-# INLINABLE (<*>) #-}

-- | Make a non-monadic reduce monadic.  Used to match types in the final fold when the unpack step is monadic
-- but reduce is not.
generalizeReduce
  :: Monad m => Reduce 'Nothing k h x e -> Reduce ( 'Just m) k h x e
generalizeReduce (Reduce     f) = ReduceM $ \k -> return . f k
generalizeReduce (ReduceFold f) = ReduceFoldM $ FL.generalize . f

-- | simplifies the unpack only map-reductions 
class IdReducer (mm :: Maybe (Type -> Type)) where
  idReducer :: Reduce mm k h x (h x)

instance IdReducer 'Nothing where
  idReducer = Reduce $ \_ x -> x

instance Monad m => IdReducer ('Just m) where
  idReducer = ReduceM $ \_ x -> return x

-- | Put all the pieces together and create the fold
mapReduceFold
  :: (Foldable h, Monoid e, ec e, Functor (MapFoldT mm x))
  => Gatherer ec gt k y (h z)
  -> MapStep mm x gt
  -> Reduce mm k h z e
  -> MapFoldT mm x e
mapReduceFold gatherer' ms reducer = case reducer of
  Reduce f -> fmap (gFoldMapWithKey gatherer' f) $ mapFold ms
  ReduceFold f ->
    fmap (gFoldMapWithKey gatherer' (\k hx -> FL.fold (f k) hx)) $ mapFold ms
  ReduceM f -> monadicMapFoldM (gFoldMapWithKeyM gatherer' f) $ mapFold ms
  ReduceFoldM f ->
    monadicMapFoldM (gFoldMapWithKeyM gatherer' (\k hx -> FL.foldM (f k) hx))
      $ mapFold ms
{-# INLINABLE mapReduceFold #-}

-- | Put all pieces together when the step and gather are already combined in a MapGather
mapGatherReduceFold
  :: (Foldable h, Monoid e, ec e, Functor (MapFoldT mm x))
  => MapGather mm x ec gt k y (h z)
  -> Reduce mm k h z e
  -> MapFoldT mm x e
mapGatherReduceFold (MapGather gatherer' mapStep') =
  mapReduceFold gatherer' mapStep'
{-# INLINABLE mapGatherReduceFold #-}

-- | Helper for the traversal step in monadic folds
monadicMapFoldM :: Monad m => (a -> m b) -> FL.FoldM m x a -> FL.FoldM m x b
monadicMapFoldM f (FL.FoldM step begin done) = FL.FoldM step begin done'
  where done' x = done x >>= f
{-# INLINABLE monadicMapFoldM #-}

