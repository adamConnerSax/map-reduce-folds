# map-reduce-folds v 0.1.0.0
## Introduction
map-reduce-folds is an attempt to find a good balance between simplicity, performance, and flexibility for simple map/reduce style operations on a data set, always initially a foldable container ```f``` of some type ```x``` (e.g., ```[x]```). The goal of the package is to create an efficient fold for the computation and package that via the types in the [foldl](http://hackage.haskell.org/package/foldl-1.4.5/docs/Control-Foldl.html) package.  
Folds are a good abstraction for looping over rows of data.  They're consequently also useful, not least because folds can be composed Applicatively, which makes it simple to do many such operations on the same data and loop over it only once.

In this package, a map/reduce computation is broken down into 4 steps:
1. Unpacking.  In the unpacking step, represented by the (pure) type ```Unpack x y```, or the (effectful) type ```UnpackM m x y```. The unpacking step can be used to filter rows, or "melt" them (turning each row into several) before processing.  The underlying function represented by an unpack is either a filter, ```x->Bool```, or a more complex unpacking ```Foldable g => x -> g y``` (or ```(Monad m, Foldable g) => x -> m (g y)``` in the effectful case).
2. Assigning.  In the asigning step, each ```y``` which resulted from the unpacking is assigned a key-part and a data-part, that is, ```Assign k y c``` wraps a function ```y -> (k,c)``` (and ```AssignM m k y c``` wraps a ```Monad m => y -> m (k, c)```). This might be as simple as identifying which columns in your data should act as keys.
3. Grouping.  In the grouping step, the collection is grouped by key, that is we transform, e.g., ```[(k,c)]``` to ```[(k, Data.Sequence.Seq c)]``` (NB: The internal representation need not be ```[]```, though there is a List engine which does use ```[]``` as its internal type). 
4. Reducing.  In the reduction step, represented by the type ```Reduce k c d``` (or ```ReduceM m k c d```), a (possibly k-dependent) function or fold maps the grouped collection of ```c``` for each key to some type ```d``` resulting in an output which is some collection of ```d```.


The "Engines" assemble unpack, assign, group and reduce steps to build a fold from a foldable containing ```x``` to some collection of ```d```.  This library provides several "engines," tailored to preferred choice of output container, though I recommend use of the [Streamly](http://hackage.haskell.org/package/streamly) engine because it is consistently fastest in my tests so far.

For example:

```haskell
import qualified Control.MapReduce.Simple          as MR
import qualified Control.MapReduce.Engine.Streamly as MRS
import qualified Control.Foldl                     as FL
import qualified Data.Map                          as M

unpack = MR.filterUnpack even -- filter, keeping even numbers
isMultipleOf3 x = (x `mod` 3) == 0 
assign = MR.assign isMultipleOf3 id -- assign each number a key of True or False depending on whether it's a multiple of 3
reduce = MR.foldAndRelabel FL.sum M.singleton -- sum each group and then create a Map for later combining

-- of the first n integers, compute the sum of even numbers which are mutliples of 3 
-- and the sum of all the even numbers which are not multiples of 3. 
result :: Map Bool Int
result n = FL.fold mrFold [1..n] 
  where
    mrFold = MRS.concatStreamFold $ MRS.streamlyEngine unpack assign reduce
``` 

In one sense this package is trivial.  Looking at the engine code, we simply compose the mapping/filtering of the unpack step (with a concat, if necessary after unpacking) with the grouping step and then the reducing step.  One could certainly hand code that without too much trouble. But there are some fiddly bits to getting all the types to line up, especially once any of the steps is effectful. 

I wrote this version of the library after writing so many versions of exactly that code that I thought it would be useful to simplify writing new ones.  And they nearly all follow the pattern made explicit here: unpack and/or filter, assign a key, group, reduce each group.  Having it packaged this way makes such operations simple to write and, better, encourages you to write the parts in ways you are likely to re-use.  Having the result be a fold makes applicative composition straightforward.  The library also supports making the reduce step itself a foldl-style fold, and then allows applicative composition within just that step.  

Once any of the steps is effectful, you need to use effectful versions of all of them so the library provides ```generalizeUnpack :: Unpack x y -> UnpackM m x y```, ```generalizeAssign :: Assign k y c -> AssignM m k y c``` and ```generalizeReduce :: Reduce k c d -> ReduceM m k c d``` to convert whichever steps are pure so they can be used to build an effectful fold.

The library is still experimental and as such, it supplies a variety of "engines" which combine the steps above into a fold.  The engines differ in their internal representation of the data as it's processed.  There is currently a list engine, a vector engine, a Streaming engine and a Streamly engine. It also reflects some experimentation with the grouping step, essentially ```Foldable g => g (k,c) -> g (k, Data.Sequence.Seq c)``` where ```k``` has an instance of ```Ord``` or ```Eq``` and ```Hashable``` (or ```Grouping``` from ```Data.Discrimination```).  The grouping function is left as a parameter to the engines and reasonable default implementations are provided for ordered and hashable keys.  Experimental implementations using a mutable hashtable for hashable keys and using ```Data.Discrimination.groupWith``` for keys with a ```Data.Discrimination.Grouping``` instance are provided for *only* the Streamly engine.  Neither of these is faster in my--admittedly cursory--tests.

The library has the type definitions for the steps in Control.MapReduce.Core, and then the separate Engines in the Control.MapReduce.Engines.XXX.  You can simply import Control.MapReduce.Simple which provides some helpers for building the various steps and a simplified choice of engines, all returning ```[d]``` or ```Monad m => m [d]```.

One note:  It's frequently the case that the result I want involves reducing each group to some monoidal type and then combining those all via ```fold```.  That fold can be written directly via ```Control.MapReduce.Simple.concatFold :: (Monoid d, Foldable g) => FL.Fold a (g d) -> FL.Fold a d``` and 
```Control.MapReduce.Simple.concatFoldM :: (Monad m, Monoid d, Foldable g) => FL.FoldM m a (g d) -> FL.FoldM m a d``` 
These are useful for combining results into a map, e.g. For the stream versions (Streaming and Streamly) I also provide direct ```concatStreamFold``` and ```concatStreamFoldM``` which transform the map/reduce folds returning streams of monoids into map/reduce folds returning the mappended monoid.  These functions may be more performant as they use each library's own fold functions.

Some simple examples are in "examples/listStats.hs" and some others are in the benchmark suite in "bench/MapReduce.hs". 

There is a wrapper for [Frames](http://hackage.haskell.org/package/Frames) available [here](https://github.com/adamConnerSax/Frames-map-reduce).

