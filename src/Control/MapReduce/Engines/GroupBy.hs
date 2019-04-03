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
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE TemplateHaskell       #-}
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
  , groupByHR
  , groupByNaiveInsert
  , groupByNaiveBubble
  , groupByNaiveInsert'
  , groupByNaiveBubble'
  , groupByInsert
  , groupByBubble
  , groupByInsert'
  , groupByBubble'
  , groupByTree1
  , groupByNaiveInsert2 -- this one doesn't work!
  )
where

import           Data.Function                  ( on )
import           Data.Maybe                     ( fromMaybe )
import qualified Data.List                     as L
import           Data.Functor.Foldable         as RS
import           Data.Functor.Foldable          ( Fix(..)
                                                , unfix
                                                , ListF(..)
                                                )
import           Data.Functor.Foldable.TH      as RS
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import           Control.Arrow                  ( second
                                                , (&&&)
                                                , (|||)
                                                )
import qualified Control.Foldl                 as FL

{-
We always (?) need to begin with fmap promote so that the elements are combinable.
It might be faster to do this in-line but it seems to complicate things...
-}
promote :: (k, v) -> (k, [v])
promote (k, v) = (k, [v])
{-# INLINABLE promote #-}

-- Fix a fully polymorphic version to our types
specify
  :: Ord k
  => (  ((k, [v]) -> (k, [v]) -> Ordering)
     -> ((k, [v]) -> (k, [v]) -> (k, [v]))
     -> t
     )
  -> t
specify f = f (compare `on` fst) (\(k, x) (_, y) -> (k, x <> y))
{-# INLINABLE specify #-}

{-
Following <https://www.cs.ox.ac.uk/ralf.hinze/publications/Sorting.pdf>,
we'll start with a Naive sort of a list

we want [x] -> [x] (where the rhs has equal elements grouped via some (x -> x -> x), e.g. (<>)
or (Fix (ListF x) -> List x)
recall:  fold :: (f a -> a) -> (Fix f -> a)
in this case f ~ (ListF x) and a ~ [x] 
we need an algebra, (f a -> a), that is  (ListF x [x] -> [x])
or (ListF x [x] -> Fix (ListF x))
This algebra is also an unfold.
recall: unfold (b -> g b) -> (b -> Fix g)
in this case, b ~ ListF x [x] and g ~ ListF x
so the required co-algebra has the form (ListF x [x] -> ListF x (ListF x [x]))
-}
coalg1
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a [a]
  -> ListF a (ListF a [a])
coalg1 _   _ Nil               = Nil
coalg1 _   _ (Cons a []      ) = Cons a Nil
coalg1 cmp f (Cons a (a' : l)) = case cmp a a' of
  LT -> Cons a (Cons a' l)
  GT -> Cons a' (Cons a l)
  EQ -> Cons (f a a') (RS.project l)
{-# INLINABLE coalg1 #-}

groupByNaiveInsert :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveInsert = RS.fold (RS.unfold (specify coalg1)) . fmap promote
{-# INLINABLE groupByNaiveInsert #-}

{-
now we do this in the other order
we want [x] -> [x] (wherer the rhs is grouped according to some (x -> x -> x), e.g., (<>)
or ([x] -> Fix (ListF x)), which is an unfold, with coalgebra ([x] -> ListF x [x])
but this co-algebra is also of the form (Fix (ListF x) -> ListF x [x])
which is a fold with algebra (ListF x (ListF x [x]) -> ListF x [x])
-}
alg1
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a (ListF a [a])
  -> ListF a [a]
alg1 _   _ Nil                   = Nil
alg1 _   _ (Cons a Nil         ) = Cons a []
alg1 cmp f (Cons a (Cons a' as)) = case cmp a a' of
  LT -> Cons a (a' : as)
  GT -> Cons a' (a : as)
  EQ -> Cons (f a a') as
{-# INLINABLE alg1 #-}

groupByNaiveBubble :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveBubble = RS.unfold (RS.fold (specify alg1)) . fmap promote
{-# INLINABLE groupByNaiveBubble #-}

{-
Some notes at this point:
1. Naive bubble is much faster than naive insert.  I think because we do the combining sooner?
2. coalg1 and and alg1 are almost the same. Let's explore this for a bit, ignoring the cmp and combine arguments
We look at the type of coalg after unrolling one level of Fix, i.e.,
since [a] = Fix (ListF a), we have `unfix [a] :: ListF a (Fix (ListF a)) ~ ListF a [a]
coalg1 :: ListF a [a] -> ListF a (ListF a [a]), so (coalg1 . fmap Fix) :: ListF a (ListF a [a]) -> ListF a (ListF a [a])
alg1 ::  ListF a (ListF a [a]) -> ListF a [a], so (fmap unfix . alg1) ::  ListF a (ListF a [a]) -> ListF a (ListF a [a])
which suggests that there is some function, following the paper, we'll call it "swap" such that
coalg1 . fmap Fix = swap = fmap unfix . alg1, or coalg1 = swap . fmap unfix and alg1 = fmap Fix . swap
Because the lhs and rhs lists look the same, this looks like an identity.  But the rhs is sorted, while the lhs is not.
-}
swap
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a (ListF a [a])
  -> ListF a (ListF a [a])
swap _   _ Nil                   = Nil
swap _   _ (Cons a Nil         ) = Cons a Nil
swap cmp f (Cons a (Cons a' as)) = case cmp a a' of
  LT -> Cons a (Cons a' as) -- already in order
  GT -> Cons a' (Cons a as) -- need to swap
  EQ -> Cons (f a a') (RS.project as)
{-# INLINABLE swap #-}

groupByNaiveInsert' :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveInsert' =
  RS.fold (RS.unfold (specify swap . fmap RS.project)) . fmap promote
{-# INLINABLE groupByNaiveInsert' #-}

groupByNaiveBubble' :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveBubble' =
  RS.unfold (RS.fold (fmap RS.embed . specify swap)) . fmap promote
{-# INLINABLE groupByNaiveBubble' #-}


{-
As pointed out in Hinze, this is inefficient because the rhs list is already sorted.
So in the non-swap cases, we need not do any more work.
The simplest way to manage that is to use an apomorphism (an unfold) in order to stop the recursion in those cases.
The types:
fold (apo coalg) :: [a] -> [a] ~ Fix (ListF a) -> [a]
apo coalg :: (ListF a [a] -> [a]) ~ ListF a [a] -> Fix (ListF a)
coalg :: (ListF a [a] -> ListF a (Either [a] (ListF a [a]))
NB: The "Left" constructor of Either stops the recursion in that branch where the "Right" constructor continues
-}
apoCoalg
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a [a]
  -> ListF a (Either [a] (ListF a [a]))
apoCoalg _   _ Nil                = Nil
apoCoalg _   _ (Cons a []       ) = Cons a (Left []) -- could also be 'Cons a (Right Nil)'
apoCoalg cmp f (Cons a (a' : as)) = case cmp a a' of
  LT -> Cons a (Left (a' : as)) -- stop recursing here
  GT -> Cons a' (Right (Cons a as)) -- keep recursing, a may not be in the right place yet!
  EQ -> Cons (f a a') (Left as) -- ??
{-# INLINABLE apoCoalg #-}

groupByInsert :: Ord k => [(k, v)] -> [(k, [v])]
groupByInsert = RS.fold (RS.apo (specify apoCoalg)) . fmap promote
{-# INLINABLE groupByInsert #-}

{-
Now we go the other way and then get both versions as before.
unfold (para alg) :: [a] -> [a] ~ [a] -> Fix (ListF a)
para alg :: [a] -> ListF a [a] ~ Fix (ListF a) -> ListF a [a]
alg :: ListF a ([a],ListF a [a]) -> ListF a [a]
-}
paraAlg
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a ([a], ListF a [a])
  -> ListF a [a]
paraAlg _   _ Nil                       = Nil
paraAlg _   _ (Cons a (_, Nil        )) = Cons a []
paraAlg cmp f (Cons a (_, Cons a' as')) = case cmp a a' of
  LT -> Cons a (a' : as')
  GT -> Cons a' (a : as')
  EQ -> Cons (f a a') as'
{-# INLINABLE paraAlg #-}

groupByBubble :: Ord k => [(k, v)] -> [(k, [v])]
groupByBubble = RS.unfold (RS.para (specify paraAlg)) . fmap promote
{-# INLINABLE groupByBubble #-}

{-
We observe, as before, apoCoalg and paraAlg are very similar, though it's less clear here.
But let's unroll one level of Fix:
apoCoalg :: ListF a (ListF a [a]) -> ListF (Either [a] (ListF a [a]))
paraAlg :: ListF a ([a], ListF a [a]) -> ListF a (ListF a [a])
So, if we had
swop :: ListF a ([a], ListF a [a]) -> ListF (Either [a] (ListF a [a]))
we could write
apoCoalg = swop . fmap (RS.embed &&& id)
paraAlg = fmap (id ||| RS.embed) . swop 
-}

swop
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a ([a], ListF a [a])
  -> ListF a (Either [a] (ListF a [a]))
swop _   _ Nil                        = Nil
swop _   _ (Cons a (as, Nil        )) = Cons a (Left as)
swop cmp f (Cons a (as, Cons a' as')) = case cmp a a' of
  LT -> Cons a (Left as)
  GT -> Cons a' (Right (Cons a as'))
  EQ -> Cons (f a a') (Left as')
{-# INLINABLE swop #-}

groupByInsert' :: Ord k => [(k, v)] -> [(k, [v])]
groupByInsert' =
  RS.fold (RS.apo (specify swop . fmap (id &&& RS.project))) . fmap promote
{-# INLINABLE groupByInsert' #-}

groupByBubble' :: Ord k => [(k, v)] -> [(k, [v])]
groupByBubble' =
  RS.unfold (RS.para (fmap (id ||| RS.embed) . specify swop)) . fmap promote
{-# INLINABLE groupByBubble' #-}




{-
Now we try to do better by unfolding to a Tree and folding to a list
which makes for fewer comparisons.
We'd like to do all the combining as we make the tree but we don't here because the tree forks any time the list elements aren't equal.
So we combine any equal ones that are adjacent on the way down.  Then combine the rest as we recombine that tree into a list.
-}
data Tree a where
  Tip :: Tree a
  Leaf :: a -> Tree a
  Fork :: Tree a -> Tree a -> Tree a deriving (Show)

RS.makeBaseFunctor ''Tree

{-
We begin by unfolding to a Tree.
unfold coalg :: ([a] -> Tree a) ~ ([a] -> Fix (TreeF a))
coalg :: ([a] -> TreeF a [a])
and we note that this coalgebra is a fold
fold alg :: ([a] -> TreeF a [a]) ~ (Fix (ListF a) -> TreeF a [a])
alg :: (ListF a (TreeF a [a]) -> TreeF a [a])
-}
toTreeAlg
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a (TreeF a [a])
  -> TreeF a [a]
toTreeAlg _   _ Nil                 = TipF
toTreeAlg _   _ (Cons a TipF      ) = LeafF a
toTreeAlg cmp f (Cons a (LeafF a')) = case cmp a a' of
  LT -> ForkF [a] [a']
  GT -> ForkF [a'] [a]
  EQ -> LeafF (f a a')
toTreeAlg cmp f (Cons a (ForkF ls rs)) = ForkF (a : rs) ls


toTreeCoalg :: (a -> a -> Ordering) -> (a -> a -> a) -> [a] -> TreeF a [a]
toTreeCoalg cmp f = RS.fold (toTreeAlg cmp f)

{-
fold alg :: (Tree a -> [a]) ~ (Fix (TreeF a) -> [a])
alg :: (TreeF a [a] -> [a]) ~ (TreeF a [a] -> Fix (ListF a))
and we note that this algebra is an unfold
unfold coalg :: TreeF a [a] -> Fix (ListF a)
coalg :: TreeF a [a] -> ListF a (TreeF a [a])
-}
toListCoalg
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> TreeF a [a]
  -> ListF a (TreeF a [a])
toListCoalg _   _ TipF                        = Nil
toListCoalg _   _ (LeafF a                  ) = Cons a TipF
toListCoalg _   _ (ForkF []       []        ) = Nil
toListCoalg _   _ (ForkF (a : as) []        ) = Cons a (ForkF [] as)
toListCoalg _   _ (ForkF []       (a  : as )) = Cons a (ForkF [] as)
toListCoalg cmp f (ForkF (a : as) (a' : as')) = case cmp a a' of
  LT -> Cons a (ForkF as (a' : as'))
  GT -> Cons a' (ForkF (a : as) as')
  EQ -> Cons (f a a') (ForkF as as')
{-# INLINABLE toListCoalg #-}

toListAlg :: (a -> a -> Ordering) -> (a -> a -> a) -> TreeF a [a] -> [a]
toListAlg cmp f = RS.unfold (toListCoalg cmp f)
{-# INLINABLE toListAlg #-}


groupByTree1 :: Ord k => [(k, v)] -> [(k, [v])]
groupByTree1 = RS.hylo (specify toListAlg) (specify toTreeCoalg) . fmap promote
{-# INLINABLE groupByTree1 #-}

{-
treeCoalg
  :: (a -> a -> Ordering)
  -> (a -> a -> a)
  -> ListF a (Tree a)
  -> TreeF a (ListF a (Tree a))
treeCoalg _   _ Nil        = TNil
treeCoalg cmp f (Cons a t) = case RS.project t of
  TNil    -> Leaf a
  Leaf a' -> case compare a a' of
    LT -> Fork (Cons a tNil) (Cons a' tNil)
    GT -> Fork (Cons a' tNil) (Cons a tNil)
    EQ -> Leaf (Cons (f a a') tNil)
  Fork tl tr -> Fork (Cons a tr) (Cons tl) -- reverse the branches for balance ??

listToGroupedTree :: [(k, v)] -> Tree (k, [v])
listToGroupedTree = RS.fold (RS.unfold treeCoalg) . fmap promote
-}


{-
What if we try to do the x -> [x] in-line?
We want [(k,v)] -> [(k,[v])]
or (Fix (ListF (k,v)) -> Fix (ListF (k,[v])))
as a fold :: (f a -> a) -> (Fix f -> a) with f ~ ListF (k,v) and a ~ [(k,[v])]
so the algebra has the form ListF (k,v) [(k,[v])] -> [(k,[v])] or ListF (k,v) [(k,[v])] -> Fix (ListF (k,[v]))
-}
alg2 :: Ord k => ListF (k, v) [(k, [v])] -> [(k, [v])]
alg2 Nil                           = []
alg2 (Cons (k, v) []             ) = [(k, [v])]
alg2 (Cons (k, v) ((k', vs) : xs)) = case compare k k' of
  LT -> (k, [v]) : (k', vs) : xs
  GT -> (k', vs) : alg2 (Cons (k, v) xs)
  EQ -> (k, v : vs) : xs
{-# INLINABLE alg2 #-}

groupByNaiveInsert2 :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveInsert2 = RS.fold alg2
{-# INLINABLE groupByNaiveInsert2 #-}








{-
data TreeF a r where
  Nil :: TreeF a r
  Leaf :: a -> TreeF a r
  Node :: r -> r -> TreeF a r deriving (Show, Functor)

type Tree a = Fix (TreeF a)
-}
-- fold a Foldable f => f (k,v) into Tree (k,[v])
-- we need an algebra (Fix () -> Tree (k,[v]))

-- hand-rolled from list functions
groupByHR :: Ord k => [(k, v)] -> [(k, [v])]
groupByHR
  = let
      fld = FL.Fold step begin done
       where
        sameKey k mk = fromMaybe False (fmap (== k) mk)
        step ([]               , _    ) (k, v) = ([(k, [v])], Just k)
        step (ll@((_, vs) : xs), mCurr) (k, v) = if sameKey k mCurr
          then ((k, v : vs) : xs, mCurr)
          else ((k, [v]) : ll, Just k)
        begin = ([], Nothing)
        done  = fst
    in  FL.fold fld . L.sortBy (compare `on` fst)
{-# INLINABLE groupByHR #-}



-- from <https://twanvl.nl/blog/haskell/generic-merge>

-- list merge, preserving ordering of keys and using semigroup (<>) when keys are equal
groupByTVL :: Ord k => [(k, v)] -> [(k, [v])]
groupByTVL = mergeSortUnion . fmap (second $ pure @[])
{-# INLINABLE groupByTVL #-}

mergeSemi :: (Ord k, Semigroup w) => [(k, w)] -> [(k, w)] -> [(k, w)]
mergeSemi = unionByWith (\a b -> compare (fst a) (fst b))
                        (\(k, w1) (_, w2) -> (k, w1 <> w2))
{-# INLINABLE mergeSemi #-}

unionByWith :: (a -> a -> Ordering) -> (a -> a -> a) -> [a] -> [a] -> [a]
unionByWith cmp f = mergeByR cmp (\a b c -> f a b : c) (:) (:) []
{-# INLINABLE unionByWith #-}

split :: [a] -> ([a], [a])
split (x : y : zs) = let (xs, ys) = split zs in (x : xs, y : ys)
split xs           = (xs, [])
{-# INLINABLE split #-}

mergeSortUnion :: Ord k => [(k, [v])] -> [(k, [v])]
mergeSortUnion []  = []
mergeSortUnion [x] = [x]
mergeSortUnion xs =
  let (ys, zs) = split xs in mergeSemi (mergeSortUnion ys) (mergeSortUnion zs)
{-# INLINABLE mergeSortUnion #-}

mergeByR
  :: (a -> b -> Ordering)  -- ^ cmp: Comparison function
  -> (a -> b -> c -> c)    -- ^ fxy: Combine when a and b are equal
  -> (a -> c -> c)         -- ^ fx:  Combine when a is less
  -> (b -> c -> c)         -- ^ fy:  Combine when b is less
  -> c                     -- ^ z:   Base case
  -> [a]
  -> [b]
  -> c       -- ^ Argument lists and result list
mergeByR cmp fxy fx fy z = go
 where
  go []       ys       = foldr fy z ys
  go xs       []       = foldr fx z xs
  go (x : xs) (y : ys) = case cmp x y of
    LT -> fx x (go xs (y : ys))
    EQ -> fxy x y (go xs ys)
    GT -> fy y (go (x : xs) ys)
{-# INLINABLE mergeByR #-}


