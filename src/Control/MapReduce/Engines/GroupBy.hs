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
  , groupByHR
  , groupByNaiveInsert
  , groupByNaiveBubble
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
import qualified Data.Foldable                 as F
import           Data.Hashable                  ( Hashable )
import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL


-- we'll start with recursion schemes with a Naive sort of a list

-- we want [x] -> [x] (where the rhs has equal elements grouped via some (x -> x -> x), e.g. (<>)
-- or (Fix (ListF x) -> List x)
-- recall:  fold :: (f a -> a) -> (Fix f -> a)
-- in this case f ~ (ListF x) and a ~ [x] 
-- we need an algebra, (f a -> a), that is  (ListF x [x] -> [x])
-- or (ListF x [x] -> Fix (ListF x))
-- We note, following <https://www.cs.ox.ac.uk/ralf.hinze/publications/Sorting.pdf>, that this algebra is also an unfold.
-- recall: unfold (b -> g b) -> (b -> Fix g)
-- in this case, b ~ ListF x [x] and g ~ ListF x
-- so the required co-algebra has the form (ListF x [x] -> ListF x (ListF x [x]))
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
  EQ -> Cons (f a a') lf
   where
    lf = case l of
      []       -> Nil
      (x : xs) -> Cons x xs
{-# INLINABLE coalg1 #-}

groupByNaiveInsert :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveInsert
  = RS.fold
      (RS.unfold (coalg1 (compare `on` fst) (\(k, x) (_, y) -> (k, x <> y))))
    . fmap promote
{-# INLINABLE groupByNaiveInsert #-}

-- now we do this in the other order
-- we want [x] -> [x] (wherer the rhs is grouped according to some (x -> x -> x), e.g., (<>)
-- or ([x] -> Fix (ListF x)), which is an unfold, with coalgebra ([x] -> ListF x [x])
-- but this co-algebra is also of the form (Fix (ListF x) -> ListF x [x])
-- which is a fold with algebra (ListF x (ListF x [x]) -> ListF x [x])
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

groupByNaiveBubble :: Ord k => [(k, v)] -> [(k, [v])]
groupByNaiveBubble =
  RS.unfold (RS.fold (alg1 (compare `on` fst) (\(k, x) (_, y) -> (k, x <> y))))
    . fmap promote
{-# INLINABLE groupByNaiveBubble #-}


-- try to do the promoting in-line
-- we want [(k,v)] -> [(k,[v])]
-- or (Fix (ListF (k,v)) -> Fix (ListF (k,[v])))
-- as a fold :: (f a -> a) -> (Fix f -> a) with f ~ ListF (k,v) and a ~ [(k,[v])]
-- so the algebra has the form ListF (k,v) [(k,[v])] -> [(k,[v])] or ListF (k,v) [(k,[v])] -> Fix (ListF (k,[v]))

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

-- since [(k,[v])] ~ Fix (ListF (k,[v])), alg2 is an unfold :: (b -> g b) -> (b -> Fix g) with g ~ ListF (k,[v]) and b ~ ListF (k,v) [(k,[v])]
-- for which we need a coalgebra: (ListF (k,v) [(k,[v])] -> ListF (k,[v]) (ListF (k,v) [(k,[v])]))
-- And I can't write that??
{-
coalg2
  :: Ord k
  => ListF (k, v) [(k, [v])]
  -> ListF (k, [v]) (ListF (k, v) [(k, [v])])
coalg2 Nil                          = Nil
coalg2 (Cons (k, v) []            ) = Cons (k, [v]) Nil
coalg2 (Cons (k, v) ((k', vs) : l)) = case compare k k' of
  LT -> Cons (k, [v]) (Cons (k', vs) l)
  GT -> Cons (k', vs) (Cons (k, v) l)
  EQ -> Cons (k, v : vs) lf
   where
    lf = case l of
      []       -> Nil
      (x : xs) -> Cons x xs
-}



{-
data TreeF a r where
  Nil :: TreeF a r
  Leaf :: a -> TreeF a r
  Node :: r -> r -> TreeF a r deriving (Show, Functor)

type Tree a = Fix (TreeF a)
-}
-- fold a Foldable f => f (k,v) into Tree (k,[v])
-- we need an algebra (Fix () -> Tree (k,[v]))
promote :: (k, v) -> (k, [v])
promote (k, v) = (k, [v])

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


