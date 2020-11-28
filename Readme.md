# map-reduce-folds v 0.1.0.6

[![Build Status][travis-badge]][travis]
[![Hackage][hackage-badge]][hackage]
[![Hackage Dependencies][hackage-deps-badge]][hackage-deps]

## Introduction
map-reduce-folds is an attempt to find a good balance between simplicity, performance, and flexibility for simple map/reduce style operations on a foldable container ```f``` of some type ```x``` (e.g., ```[x]```). The goal of the package is to provide an efficient fold for the computation via the types in the [foldl](http://hackage.haskell.org/package/foldl-1.4.5/docs/Control-Foldl.html) package.  
Folds can be composed Applicatively, which makes it simple to do many such operations on the same data and loop over it only once.

In this package, a map/reduce computation is broken down into 4 steps:
1. *Unpacking*:  The unpacking step is represented by the (pure) type ```Unpack x y```, or the (effectful) type ```UnpackM m x y```. The unpacking step can be used to filter, or "melt" (turning each ```x``` into none or several ```y```) before processing.  The underlying function represented by an unpack is either a filter, ```x->Bool``` or ```x -> m Bool```, or a more complex unpacking ```Foldable g => x -> g y``` (or ```(Monad m, Foldable g) => x -> m (g y)``` in the effectful case).
2. *Assigning*:  In the asigning step, each ```y``` which resulted from the unpacking is assigned a key-part and a data-part, that is, ```Assign k y c``` wraps a function ```y -> (k,c)``` (and ```AssignM m k y c``` wraps a ```Monad m => y -> m (k, c)```).
3. *Grouping*:  In the grouping step, the collection is grouped by key, that is we transform, e.g., ```[(k,c)]``` to ```[(k, Data.Sequence.Seq c)]``` (NB: The internal representations need not be ```[]``` or ```Seq```, though there is a List engine which does use ```[]``` as its internal type and all the provided grouping functions use ```Seq```). 
4. *Reducing*:  In the reduction step, represented by the type ```Reduce k c d``` (or ```ReduceM m k c d```), a (possibly k-dependent) function or fold maps the grouped collection of ```c``` for each key to some type ```d``` resulting in an output which is some collection of ```d```.

An *Engine* assembles the unpack, assign, group and reduce steps to build a fold from a container of ```x``` to some collection of ```d```.  This library provides several engines, tailored to preferred choice of output container. I recommend use of the [Streamly](http://hackage.haskell.org/package/streamly) engine because it is consistently fastest in my tests so far.

For example:

```haskell
import qualified Control.MapReduce.Simple      as MR
import qualified Control.MapReduce.Engines.Streamly
                                               as MRS
import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M

unpack = MR.filterUnpack even -- filter, keeping even numbers

-- assign each number a key of True or False depending on whether it's a multiple of 3. Group the number itself.
isMultipleOf3 x = (x `mod` 3) == 0
assign = MR.assign isMultipleOf3 id

reduce = MR.foldAndLabel FL.sum M.singleton -- sum each group and then create a singleton Map for later combining

-- of the first n integers, compute the sum of even numbers which are multiples of 3 
-- and the sum of even numbers which are not multiples of 3. 

mrFold :: FL.Fold Int (M.Map Bool Int)
mrFold = MRS.concatStreamFold
  $ MRS.streamlyEngine MRS.groupByHashableKey unpack assign reduce

result :: Int -> M.Map Bool Int
result n = FL.fold mrFold [1 .. n]

main = putStrLn $ show $ result 10
-- output: fromList [(False,24),(True,6)]
``` 

In one sense this package is trivial.  Looking at the engine code, we simply compose the mapping/filtering of the unpack step (with a concat, if necessary after unpacking) with the grouping step and then the reducing step.  One could certainly hand code that without too much trouble. But there are some fiddly bits to getting all the types to line up, especially once any of the steps is effectful.  


I wrote this version of the library after writing so many versions of exactly that code that I thought it would be useful to simplify writing new ones.  And they nearly all follow the pattern made explicit here: unpack and/or filter, assign a key, group, reduce each group.  Having it packaged this way makes such operations simple to write and, better, encourages you to write the parts in ways you are likely to re-use.  Having the result be a fold makes applicative composition straightforward.  The library also supports making the reduce step itself a foldl-style fold, and then allows efficient applicative composition within just that step.  

Once any of the steps is effectful, you need to use effectful versions of all of them so the library provides ```generalizeUnpack :: Unpack x y -> UnpackM m x y```, ```generalizeAssign :: Assign k y c -> AssignM m k y c``` and ```generalizeReduce :: Reduce k c d -> ReduceM m k c d``` to convert whichever steps are pure so they can be used to build an effectful fold.

The library is still experimental and as such, it supplies a variety of "engines" which combine the steps above into a fold.  The engines differ in their internal representation of the data as it's processed.  There is currently a list engine, a vector engine, a Streaming engine and a Streamly engine. It also reflects some experimentation with the grouping step, essentially ```Foldable g => g (k,c) -> g (k, Data.Sequence.Seq c)``` where ```k``` has an instance of ```Ord``` or ```Eq``` and ```Hashable``` (or ```Grouping``` from ```Data.Discrimination```).  The grouping function is left as a parameter to the engines and reasonable default implementations are provided for ordered and hashable keys.  Experimental implementations using a mutable hashtable for hashable keys and using ```Data.Discrimination.groupWith``` for keys with a ```Data.Discrimination.Grouping``` instance are provided for *only* the Streamly engine.  Neither of these is faster in my--admittedly cursory--tests.

```Control.MapReduce.Core``` contains the type definitions for the steps, and the separate Engines are in ```Control.MapReduce.Engines.XXX```.  You can simply import ```Control.MapReduce.Simple``` which provides some helpers for building the various steps and a simplified choice of engines, all returning ```[d]``` or ```Monad m => m [d]```.

Notes:
1.  As in the example above, it's frequently the case that I want to reduce each group to some monoidal type and then combine those all via ```mappend```.  That can be written directly via ```Control.MapReduce.Simple.concatFold :: (Monoid d, Foldable g) => FL.Fold a (g d) -> FL.Fold a d``` and 
```Control.MapReduce.Simple.concatFoldM :: (Monad m, Monoid d, Foldable g) => FL.FoldM m a (g d) -> FL.FoldM m a d``` 
These are useful for combining results into a map, e.g. For the stream versions (Streaming and Streamly) I also provide direct ```concatStreamFold``` and ```concatStreamFoldM``` which transform the map/reduce folds returning streams of monoids into map/reduce folds returning the mappended monoid.  These functions may be more performant as they use each library's own fold functions.
2. I've looked some at making these folds concurrent, either using the ```parMap``` functionality in Control.Parallel or the stream concurrency from Streamly.  Versions of both are in the library but in my very casual testing both are slower than the serial versions.  So I've done something wrong or my test-case is not expensive enough or, probably, both.  Suggestions are welcome!

Some simple examples are in "examples/listStats.hs" and some others are in the benchmark suite in "bench/MapReduce.hs". 

There is a wrapper for [Frames](http://hackage.haskell.org/package/Frames) available [here](https://github.com/adamConnerSax/Frames-map-reduce).

_______


LICENSE (BSD-3-Clause)
_______
Copyright (c) 2018, Adam Conner-Sax, All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials provided
      with the distribution.

    * Neither the name of Adam Conner-Sax nor the names of other
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


[travis]:        <https://travis-ci.org/adamConnerSax/map-reduce-folds>
[travis-badge]:  <https://travis-ci.org/adamConnerSax/map-reduce-folds.svg?branch=master>
[hackage]:       <https://hackage.haskell.org/package/map-reduce-folds>
[hackage-badge]: <https://img.shields.io/hackage/v/map-reduce-folds.svg>
[hackage-deps-badge]: <https://img.shields.io/hackage-deps/v/map-reduce-folds.svg>
[hackage-deps]: <http://packdeps.haskellers.com/feed?needle=map-reduce-folds>
