{-|
Module      : Control.MapReduce
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
The Reduce type is also Applicative so there could be work sharing there as well:
e.g., if your `reduce :: (k -> d -> e)` has the form `reduce k :: FL.Fold d e`

These types are meant to simplify the building of "Engines" which combine them into a single efficient fold from a container of x to some container of the result.  The Engine amounts to a choice of grouping algorithm (usually just uses Data.Map or Data.HashMap) and a type in which to do the calculations and return the result. These are assembled into a Fold.

The goal is to make assembling a large family of common map/reduce patterns in a straightforward way.  At some level of complication, you may as
well write them by hand.  An in-between case would be writing the unpack function as a complex hand written filter
-}
module Control.MapReduce
  (
    -- * Core types for specifying map/reduce folds
    module Control.MapReduce.Core
    -- * default choices for grouping algorithms and intermediate types and helper functions
    -- for common cases.
  , module Control.MapReduce.Simple
  )
where
import           Control.MapReduce.Core
import           Control.MapReduce.Simple

