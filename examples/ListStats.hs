module Main where

import qualified Control.MapReduce.Simple      as MR
import qualified Control.Foldl                 as FL

ints n = take n [1 ..]

multipleOf n x = (x `mod` n) == 0

onlyEven = MR.filterUnpack even -- a filter

andTwice x = [x, 2 * x]  -- a "melt"

withTwice :: MR.Unpack Int Int
withTwice = MR.Unpack andTwice

groupByMultOf3 = MR.assign (multipleOf 3) realToFrac

-- An example using the function version
addAll :: (Foldable f, Num a) => f a -> a
addAll = FL.fold FL.sum


reduceSum = MR.foldAndLabel FL.sum ((,))
reduceSum' = MR.processAndLabel addAll (,)
reduceMean = MR.foldAndLabel FL.mean ((,))

-- sum of all even ints, grouped by whether or not they are mutliples of 3
sumsF = MR.mapReduceFold onlyEven groupByMultOf3 reduceSum
sumsF' = MR.mapReduceFold onlyEven groupByMultOf3 reduceSum'

-- mean of all even ints, grouped by whether or not they are mutliples of 3
meansF = MR.mapReduceFold onlyEven groupByMultOf3 reduceMean

-- loop over initial list once but group each time
sumsAndMeansF = (,) <$> sumsF <*> meansF

-- loop over initial list once and group only once.
sumAndMeanEachF =
  MR.mapReduceFold onlyEven groupByMultOf3 ((,) <$> reduceSum <*> reduceMean)

-- sum and mean of all ints--and twice each int--grouped by whether or not they are multiples of 3.
sumAndMeanWithTwiceF =
  MR.mapReduceFold withTwice groupByMultOf3 ((,) <$> reduceSum <*> reduceMean)


{-
We combine all the folds we want to do into one fold.  So we loop over the input list only once here.
We group it for each fold.  But the last two, sumAndMeanEach and sumAndMeanWithTwice shows how we can avoid that as well.
-}

main :: IO ()
main = do
  let (s, s', m, sm, sme, smd) = MR.fold
        (   (,,,,,)
        <$> sumsF
        <*> sumsF'
        <*> meansF
        <*> sumsAndMeansF
        <*> sumAndMeanEachF
        <*> sumAndMeanWithTwiceF
        )
        (ints 100)
  putStrLn $ "Sums: " ++ show s
  putStrLn $ "Sums': " ++ show s'
  putStrLn $ "Means: " ++ show m
  putStrLn $ "Sums & Means: " ++ show sm
  putStrLn $ "Sum & Mean of each: " ++ show sme
  putStrLn $ "Sum & Mean of all plus doubles: " ++ show smd


{- Output:

Sums: [(False,1734.0),(True,816.0)]
Sums': [(False,1734.0),(True,816.0)]
Means: [(False,51.0),(True,51.0)]
Sums & Means: ([(False,1734.0),(True,816.0)],[(False,51.0),(True,51.0)])
Sum & Mean of each: [((False,1734.0),(False,51.0)),((True,816.0),(True,51.0))]
Sum & Mean of all plus doubles: [((False,10101.0),(False,75.38059701492539)),((True,5049.0),(True,76.5))]

-}
