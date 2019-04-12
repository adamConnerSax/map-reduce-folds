{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE PolyKinds         #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
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
import           Control.MapReduce.Engines.ParallelList
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
                                                , randomRIO
                                                )


createPairData :: Int -> IO [(Char, Int)]
createPairData n = do
  g <- newStdGen
  let randLabels = L.take n $ randomRs ('A', 'Z') g
      randInts   = L.take n $ randomRs (1, 100) g
  return $ L.zip randLabels randInts

benchPure :: (NFData b) => String -> (Int -> a) -> (a -> b) -> Benchmark
benchPure name src f =
  bench name $ nfIO $ randomRIO (1, 1) >>= return . f . src

benchIO :: (NFData b) => String -> (Int -> a) -> (a -> IO b) -> Benchmark
benchIO name src f = bench name $ nfIO $ randomRIO (1, 1) >>= f . src

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
  (MRL.listEngine MRL.groupByHashableKey
                  (MR.Filter filterPF)
                  (MR.Assign id)
                  (MR.Reduce reducePF)
  )
{-# INLINE mapReduceList #-}

mapReduceListP :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceListP = FL.fold
  (MRP.parallelListEngine 6
                          MRL.groupByHashableKey
                          (MR.Filter filterPF)
                          (MR.Assign id)
                          (MR.Reduce reducePF)
  )
{-# INLINE mapReduceListP #-}


mapReduceStreaming :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceStreaming = runIdentity . MRS.resultToList . FL.fold
  (MRS.streamingEngine MRS.groupByHashableKey
                       (MR.Filter filterPF)
                       (MR.Assign id)
                       (MR.Reduce reducePF)
  )
{-# INLINE mapReduceStreaming #-}

mapReduceStreamly :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceStreamly = runIdentity . MRSL.resultToList . FL.fold
  (MRSL.streamlyEngine MRSL.groupByHashableKey
                       (MR.Filter filterPF)
                       (MR.Assign id)
                       (MR.Reduce reducePF)
  )
{-# INLINE mapReduceStreamly #-}

mapReduceStreamlyC
  :: forall tIn tOut m g
   . (MonadAsync m, Foldable g, MRSL.IsStream tIn, MRSL.IsStream tOut)
  => g (Char, Int)
  -> m [(Char, Double)]
mapReduceStreamlyC = MRSL.resultToList . FL.fold
  ((MRSL.concurrentStreamlyEngine @tIn @tOut) MRSL.groupByHashableKey
                                              (MR.Filter filterPF)
                                              (MR.Assign id)
                                              (MR.Reduce reducePF)
  )
{-# INLINE mapReduceStreamlyC #-}

mapReduceVector :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapReduceVector = MRV.toList . FL.fold
  (MRV.vectorEngine MRV.groupByHashableKey
                    (MR.Filter filterPF)
                    (MR.Assign id)
                    (MR.Reduce reducePF)
  )
{-# INLINE mapReduceVector #-}

{-
parMapReduce :: Foldable g => g (Char, Int) -> [(Char, Double)]
parMapReduce = FL.fold
  (MRP.parallelMapReduceFold
    6
    (MR.Unpack $ \x -> if filterPF x then [x] else [])
    (MR.Assign id)
    (MR.Reduce reducePF)
  )
{-# INLINE parMapReduce #-}
-}



benchOne dat = bgroup
  "Task 1, on (Char, Int) "
  [ benchPure "direct"      (const dat) direct
  , benchPure "directFoldl" (const dat) directFoldl
  , benchPure "mapReduce ([] Engine, strict hash map)" (const dat) mapReduceList
  , benchPure "mapReduce (Streaming.Stream Engine, strict hash map)"
              (const dat)
              mapReduceStreaming
  , benchPure "mapReduce (Streamly.SerialT Engine, strict hash map)"
              (const dat)
              mapReduceStreamly
  , benchPure "mapReduce (Data.Vector Engine, strict hash map)"
              (const dat)
              mapReduceVector
  ]

benchConcurrent dat = bgroup
  "Task 1, on (Char, Int). Concurrent Engines"
  [ benchPure "list, parallel (6 threads)" (const dat) mapReduceListP
  , benchIO "streamly, parallely"
            (const dat)
            (mapReduceStreamlyC @MRSL.SerialT @MRSL.ParallelT)
  , benchIO "streamly, aheadly"
            (const dat)
            (mapReduceStreamlyC @MRSL.SerialT @MRSL.AheadT)
  , benchIO "streamly, asyncly"
            (const dat)
            (mapReduceStreamlyC @MRSL.SerialT @MRSL.AsyncT)
  ]

-- a more complex row type
createMapRows :: Int -> IO (Seq.Seq (M.Map T.Text Int))
createMapRows n = do
  g <- newStdGen
  let randInts = L.take (n + 1) $ randomRs (1, 100) g
      makeRow k =
        let l = randInts !! k
        in  if even l
              then M.fromList [("A", l), ("B", l `mod` 47), ("C", l `mod` 13)]
              else M.fromList [("A", l), ("B", l `mod` 47)]
  return
    $ Seq.unfoldr (\m -> if m > n then Nothing else Just (makeRow m, m + 1)) 0

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
  (MRSL.streamlyEngine MRSL.groupByHashableKey
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

{-
basicListP :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
basicListP = FL.fold
  (MRP.parallelMapReduceFold 6
                             (MR.Unpack unpackMF)
                             (MR.Assign assignMF)
                             (MR.foldAndRelabel reduceMFold (\k x -> (k, x)))
  )
-}

benchTwo dat = bgroup
  "Task 2, on Map Text Int "
  [ benchPure "direct" (const dat) directM
  , benchPure "map-reduce-fold ([] Engine, strict hash map, serial)"
              (const dat)
              mapReduce2List
  , benchPure
    "map-reduce-fold (Streaming.Stream Engine, strict hash map, serial)"
    (const dat)
    mapReduce2Streaming
  , benchPure
    "map-reduce-fold (Streamly.SerialT Engine, strict hash map, serial)"
    (const dat)
    mapReduce2Streamly
  , benchPure "map-reduce-fold (Data.Vector Engine, strict hash map, serial)"
              (const dat)
              mapReduce2Vector
  ]

main :: IO ()
main = do
  dat  <- createPairData 100000
  dat2 <- createMapRows 100000
  defaultMain [benchOne dat, benchConcurrent dat, benchTwo dat2]
