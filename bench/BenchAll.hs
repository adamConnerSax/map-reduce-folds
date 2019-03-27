{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes            #-}
import           Criterion.Main
import           Criterion


import           Control.MapReduce             as MR
import           Control.MapReduce.Engines     as MRE
import           Control.MapReduce.Parallel    as MRP

import           Data.Text                     as T
import           Data.List                     as L
import           Data.HashMap.Lazy             as HM
import           Data.Map                      as M
import qualified Control.Foldl                 as FL
import           Control.Arrow                  ( second )
import           Data.Foldable                 as F
import           Data.Sequence                 as Seq
import           Data.Maybe                     ( catMaybes )

createPairData :: Int -> [(Char, Int)]
createPairData n =
  let makePair k = (toEnum $ fromEnum 'A' + k `mod` 26, k `mod` 31)
  in  L.unfoldr (\m -> if m > n then Nothing else Just (makePair m, m + 1)) 0

-- For example, keep only even numbers, then compute the average of the Int for each label.
filterPF = even . snd
assignPF = id -- this is a function from the "data" to a pair (key, data-to-process).
reducePFold = FL.premap realToFrac FL.mean
reducePF k hx = (k, FL.fold reducePFold hx)

-- the most direct way I can easily think of
direct :: Foldable g => g (Char, Int) -> [(Char, Double)]
direct =
  HM.toList
    . fmap (FL.fold reducePFold)
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . F.toList
{-# INLINE direct #-}

direct2 :: Foldable g => g (Char, Int) -> [(Char, Double)]
direct2 =
  fmap (uncurry reducePF)
    . HM.toList
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . F.toList
{-# INLINE direct2 #-}


directFoldl :: Foldable g => g (Char, Int) -> [(Char, Double)]
directFoldl =
  fmap (uncurry reducePF)
    . HM.toList
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . FL.fold FL.list
{-# INLINE directFoldl #-}

directFoldl2 :: Foldable g => g (Char, Int) -> [(Char, Double)]
directFoldl2 = FL.fold
  ((fmap
     ( fmap (uncurry reducePF)
     . HM.toList
     . HM.fromListWith (<>)
     . fmap (second $ pure @[])
     . L.filter filterPF
     )
   )
    FL.list
  )
{-# INLINE directFoldl2 #-}

directFoldl3 :: Foldable g => g (Char, Int) -> [(Char, Double)]
directFoldl3 = FL.fold
  ((fmap
     ( fmap (uncurry reducePF)
     . HM.toList
     . HM.fromListWith (<>)
     . fmap (second $ pure @[])
     . L.concat
     . fmap (\x -> if filterPF x then [x] else [])
     )
   )
    FL.list
  )
{-# INLINE directFoldl3 #-}


mapReduce :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduce = FL.fold
  (MR.hashableMapReduceFold (MR.Unpack $ \x -> if filterPF x then [x] else [])
                            (MR.Assign id)
                            (MR.Reduce reducePF)
  )
{-# INLINE mapReduce #-}

parMapReduce :: Foldable g => g (Char, Int) -> [(Char, Double)]
parMapReduce = FL.fold
  (MRP.parallelMapReduceFold
    6
    (MR.Unpack $ \x -> if filterPF x then [x] else [])
    (MR.Assign id)
    (MR.Reduce reducePF)
  )
{-# INLINE parMapReduce #-}



benchOne dat = bgroup
  "Task 1, on (Char, Int) "
  [ bench "direct" $ nf direct dat
  , bench "direct2" $ nf direct2 dat
  , bench "directFoldl" $ nf directFoldl dat
  , bench "directFoldl2" $ nf directFoldl2 dat
  , bench "directFoldl3" $ nf directFoldl3 dat
  , bench "mapReduce" $ nf mapReduce dat
  , bench "parMapReduce" $ nf parMapReduce dat
  ]

-- a more complex row type
createMapRows :: Int -> Seq.Seq (M.Map T.Text Int)
createMapRows n =
  let makeRow k = if even k
        then M.fromList [("A", k), ("B", k `mod` 47), ("C", k `mod` 13)]
        else M.fromList [("A", k), ("B", k `mod` 47)]
  in  Seq.unfoldr (\m -> if m > n then Nothing else Just (makeRow m, m + 1)) 0

-- unpack: if A and B and C are present, unpack to Just (A,B,C), otherwise Nothing
unpackMF :: M.Map T.Text Int -> Maybe (Int, Int, Int)
unpackMF m = do
  a <- M.lookup "A" m
  b <- M.lookup "B" m
  c <- M.lookup "C" m
  return (a, b, c)

-- group by the value of "C"
assignMF :: (Int, Int, Int) -> (Int, (Int, Int))
assignMF (a, b, c) = (c, (a, b))

-- compute the average of the sum of the values in A and B for each group
reduceMFold :: FL.Fold (Int, Int) Double
reduceMFold = let g (x, y) = realToFrac (x + y) in FL.premap g FL.mean

-- return [(C, <A+B>)]

directM :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
directM =
  M.toList
    . fmap (FL.fold reduceMFold)
    . M.fromListWith (<>)
    . fmap (second (pure @[]) . assignMF)
    . catMaybes
    . fmap unpackMF
    . F.toList

basicList :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
basicList = FL.fold
  (MR.lazyHashMapListEngine (MR.Unpack unpackMF)
                            (MR.Assign assignMF)
                            (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )


basicListP :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
basicListP = FL.fold
  (MRP.parallelMapReduceFold 6
                             (MR.Unpack unpackMF)
                             (MR.Assign assignMF)
                             (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )

benchTwo dat = bgroup
  "Task 2, on Map Text Int "
  [ bench "direct" $ nf directM dat
  , bench "map-reduce-fold (lazy hash map, serial)" $ nf basicList dat
  , bench "map-reduce-fold (lazy hash map, parallel)" $ nf basicListP dat
  ]

main :: IO ()
main = defaultMain
  [benchOne $ createPairData 100000, benchTwo $ createMapRows 100000]
