{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes            #-}
import           Criterion.Main
import           Criterion


import           Control.MapReduce             as MR
import           Control.MapReduce.Engines.GroupBy
                                               as MRG
import           Control.MapReduce.Engines.List
                                               as MRL
import           Control.MapReduce.Engines.Streaming
                                               as MRS
import           Control.MapReduce.Engines.Streamly
                                               as MRSL
import           Control.MapReduce.Engines.Vector
                                               as MRV
import           Control.MapReduce.Engines.Parallel
                                               as MRP
import           Data.Functor.Identity          ( runIdentity )
import           Data.Text                     as T
import           Data.List                     as L
import           Data.HashMap.Lazy             as HM
import           Data.Map                      as M
import qualified Control.Foldl                 as FL
import           Control.Arrow                  ( second )
import           Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(Identity)
                                                , runIdentity
                                                )
import           Data.Sequence                 as Seq
import           Data.Maybe                     ( catMaybes )
import           System.Random                  ( newStdGen
                                                , randomRs
                                                )

createPairData :: Int -> IO [(Char, Int)]
createPairData n = do
  g <- newStdGen
  let randLabels = L.take n $ randomRs ('A', 'Z') g
      randInts   = L.take n $ randomRs (1, 100) g
  return $ L.zip randLabels randInts

-- For example, keep only even numbers, then compute the average of the Int for each label.
filterPF = even . snd
assignPF = id -- this is a function from the "data" to a pair (key, data-to-process).
reducePFold = FL.premap realToFrac FL.mean
reducePF k hx = (k, FL.fold reducePFold hx)

direct :: Foldable g => g (Char, Int) -> [(Char, Double)]
direct =
  fmap (uncurry reducePF)
    . HM.toList
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . F.toList
{-# INLINE direct #-}


directFoldl :: Foldable g => g (Char, Int) -> [(Char, Double)]
directFoldl =
  fmap (uncurry reducePF)
    . HM.toList
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . FL.fold FL.list
{-# INLINE directFoldl #-}



mapReduceList :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceList = FL.fold
  (MRL.listEngine MRL.groupByHashedKey
                  (MR.Filter filterPF)
                  (MR.Assign id)
                  (MR.Reduce reducePF)
  )
{-# INLINE mapReduceList #-}


mapReduceStreaming :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceStreaming = runIdentity . MRS.resultToList . FL.fold
  (MRS.streamingEngine MRS.groupByHashedKey
                       (MR.Filter filterPF)
                       (MR.Assign id)
                       (MR.Reduce reducePF)
  )
{-# INLINE mapReduceStreaming #-}

mapReduceStreamly :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceStreamly = runIdentity . MRSL.resultToList . FL.fold
  ((MRSL.streamlyEngine @MRSL.SerialT) MRSL.groupByHashableKey
                                       (MR.Filter filterPF)
                                       (MR.Assign id)
                                       (MR.Reduce reducePF)
  )
{-# INLINE mapReduceStreamly #-}

mapReduceVector :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceVector = MRV.toList . FL.fold
  (MRV.vectorEngine MRV.groupByHashedKey
                    (MR.Filter filterPF)
                    (MR.Assign id)
                    (MR.Reduce reducePF)
  )
{-# INLINE mapReduceVector #-}


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
  , bench "directFoldl" $ nf directFoldl dat
  , bench "mapReduce ([] Engine, strict hash map)" $ nf mapReduceList dat
  , bench "mapReduce (Streaming.Stream Engine, strict hash map)"
    $ nf mapReduceStreaming dat
  , bench "mapReduce (Streamly.SerialT Engine, strict hash map)"
    $ nf mapReduceStreamly dat
  , bench "mapReduce (Data.Vector Engine, strict hash map)"
    $ nf mapReduceVector dat
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

mapReduce2List :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
mapReduce2List = FL.fold
  (MRL.listEngine MRL.groupByHashableKey
                  (MR.Unpack unpackMF)
                  (MR.Assign assignMF)
                  (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )


mapReduce2Streaming :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
mapReduce2Streaming = runIdentity . MRS.resultToList . FL.fold
  (MRS.streamingEngine MRS.groupByHashableKey
                       (MR.Unpack unpackMF)
                       (MR.Assign assignMF)
                       (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )

mapReduce2Streamly :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
mapReduce2Streamly = runIdentity . MRSL.resultToList . FL.fold
  ((MRSL.streamlyEngine @MRSL.SerialT)
    MRSL.groupByHashableKey
    (MR.Unpack unpackMF)
    (MR.Assign assignMF)
    (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )

mapReduce2Vector :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
mapReduce2Vector = MRV.toList . FL.fold
  (MRV.vectorEngine MRV.groupByHashableKey
                    (MR.Unpack unpackMF)
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
  , bench "map-reduce-fold ([] Engine, strict hash map, serial)"
    $ nf mapReduce2List dat
  , bench "map-reduce-fold (Streaming.Stream Engine, strict hash map, serial)"
    $ nf mapReduce2Streaming dat
  , bench "map-reduce-fold (Streamly.SerialT Engine, strict hash map, serial)"
    $ nf mapReduce2Streamly dat
  , bench "map-reduce-fold (Data.Vector Engine, strict hash map, serial)"
    $ nf mapReduce2Vector dat
  , bench "map-reduce-fold ([] Engine, lazy hash map, parallel)"
    $ nf basicListP dat
  ]

main :: IO ()
main = do
  dat <- createPairData 100000
  let dat2 = createMapRows 100000
  defaultMain [benchOne dat, benchTwo dat2]
