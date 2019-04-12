# map-reduce-folds
## Introduction
map-reduce-folds is an attempt to find a good balance between simplicity, performance, and flexibility for simple map/reduce style operations on a data set, always initially a foldable container ```f``` of some type ```x``` (e.g., ```[x]```). The goal of the package is to create an efficient fold for the computation and package that via the types in the [foldl](http://hackage.haskell.org/package/foldl-1.4.5/docs/Control-Foldl.html) package.  
Packaging map/reduce operations as folds is natural: folds are a good abstraction of looping over rows of data.  It's consequently also useful, not least because folds can be composed Applicatively, which makes it simple to do many such operations on the same data and loop over it only once.

In this package, a map/reduce computation is broken down into 4 steps:
1. Unpacking.  In the unpacking step, represented by the (pure) type ```Unpack x y```, or the (effectful) type ```UnpackM m x y```. The unpacking step can be used to filter rows, or "melt" them (turning each row into several) before processing.  The underlying function represented by an unpack is either a filter, ```x->Bool```, or a more complex unpacking ```Foldable g => x -> g y``` (or ```(Monad m, Foldable g) => x -> m (g y)``` in the effectful case).
2. Assigning.  In the asigning step, each ```y``` which resulted from the unpacking is assigned a key-part and a data-part, that is, ```Assign k y c``` wraps a function ```y -> (k,c)``` (and ```AssignM m k y c``` wraps a ```Monad m => y -> m (k, c)```). This might be as simple as identifying which columns in your data should act as keys.
3. Grouping.  In the grouping step, the collection is grouped by key, that is we transform, e.g., ```[(k,c)]``` to ```[(k,[c])]``` (NB: The internal representation is not necessarily [], though there is a List engine which does use [] as its internal type). 
4. Reducing.  In the reduction step, represented by the type ```Reduce k c d``` (pr ```ReduceM m k c d```), a (possibly k-dependent) function or fold maps the grouped collection of ```c``` for each key to some type ```d``` resulting in an output which is some collection of ```d```.

Essentially, we assemble an unpack, assign, group and reduce step to build a fold from a foldable containing ```x``` to some collection of ```d```.  This library provides several "engines," tailored to preferred choice of output container, though I recommend use of the [Streamly](http://hackage.haskell.org/package/streamly) engine because it is consistently fastest in my tests so far.

In one sense this package is trivial.  Looking at the engine code, we simply compose the mapping/filtering of the unpack step (with a concat, if necessary after unpacking) with the grouping step and then the reducing step.  One could certainly hand code that without too much trouble.

I wrote this version of the library after writing so many versions of exactly that code that it became clear that it would be useful to simplify writing new ones.  Any they nearly all follow the pattern made explicit here: unpack and/or filter, assign a key, group, reduce each group.  Having it packaged this way makes such operations simple to write and, better, encourages you to write the parts in ways you are likely to re-use.  Having the result be a fold makes applicative composition straightforward.  The library also supports making the reduce step itself a foldl-style fold, and then allows applicative composition within just that step.  

All the applicative composition just amounts to the observation that we need only ever loop over the original data once, that if we have multiple reductions to do on the same groups, we need only do the grouping once, and that if those reductions are themselves folds, we need only loop over each grouped collection once in order to compute all the results.

The library is still experimental and as such, it supplies a variety of "engines" which combine the steps above into a fold.  The engines differ in their internal representation of the data as it's processed.  There is currently a list engine, a vector engine, a Streaming engine and a Streamly engine.  

The library has the type definitions for the steps in Control.MapReduce.Core, and then the separate Engines in the Control.MapReduce.Engines.XXX.  You can simply import Control.MapReduce.Simple which provides some helpers for building the various steps and a simplified choice of engines, all returning ```[d]``` or ```m [d]```.

One note:  It's frequently the case that the result I want involves reducing each group to some monoidal type and then combining those all via ```fold```.  That fold can be written directly via ```Control.MapReduce.Simple.concatFold :: (Monoid d, Foldable g) => FL.Fold a (g d) -> FL.Fold a d``` and 
```Control.MapReduce.Simple.concatFoldM :: (Monad m, Monoid d, Foldable g) => FL.FoldM m a (g d) -> FL.FoldM m a d``` 
These are useful for combining results into a map, e.g.

Some simple examples are in the benchmark suite. 

There is a wrapper for [Frames](http://hackage.haskell.org/package/Frames) available [here](https://github.com/adamConnerSax/Frames-map-reduce) with more examples.

