{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
import           Criterion.Main
import           Criterion
import           Control.MapReduce             as MR
import           Data.Text                     as T
import           Data.List                     as L
import           Data.HashMap.Lazy             as HM
import           Data.Map                      as M
import           Control.Foldl                 as FL
import           Control.Arrow                  ( second )
import           Data.Foldable                 as F
import           Data.Sequence                 as Seq
import           Data.Maybe                     ( catMaybes )

createPairData :: Int -> Seq.Seq (Char, Int)
createPairData n =
  let makePair k = (toEnum $ fromEnum 'A' + k `mod` 26, k `mod` 31)
  in  Seq.unfoldr (\m -> if m > n then Nothing else Just (makePair m, m + 1)) 0

-- For example, keep only even numbers, then compute the average of the Int for each label.
filterPF = even . snd
assignPF = id -- this is a function from the "data" to a pair (key, data-to-process). 
reducePFold = FL.premap realToFrac FL.mean

-- the most direct way I can easily think of
direct :: Foldable g => g (Char, Int) -> [(Char, Double)]
direct =
  HM.toList
    . fmap (FL.fold reducePFold)
    . HM.fromListWith (<>)
    . fmap (second $ pure @[])
    . L.filter filterPF
    . F.toList

-- this should be pretty close to what the mapreduce code will do
-- in particular, do all unpacking and assigning, then make a sequence of the result and then group that.
copySteps :: (Functor g, Foldable g) => g (Char, Int) -> [(Char, Double)]
copySteps =
  HM.foldrWithKey (\k m l -> (k, m) : l) []
    . fmap (FL.fold reducePFold)
    . HM.fromListWith (<>)
    . F.toList
    . fmap (second $ pure @[])
    . F.fold
    . fmap (Seq.fromList . F.toList . fmap assignPF)
    . fmap (\x -> if filterPF x then Just x else Nothing) --L.filter filterF


-- the default map-reduce
mapAllGatherEach :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapAllGatherEach = FL.fold
  (MR.basicListFold @Hashable
    (MR.filterUnpack filterPF)
    (MR.Assign assignPF)
    (MR.foldAndRelabel reducePFold (\k m -> [(k, m)]))
  )

-- use the basic parallel apparatus
mapAllGatherEachP :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapAllGatherEachP = FL.fold
  (MR.parBasicListHashableFold
    1000
    6
    (MR.filterUnpack filterPF)
    (MR.Assign assignPF)
    (MR.foldAndRelabel reducePFold (\k m -> [(k, m)]))
  )



g = MR.defaultHashableGatherer (pure @Seq)


-- try the variations on unpack, assign and fold order
-- for all but mapAllGatherEach, we need unpack to unpack to a monoid
monoidUnpackF =
  MR.Unpack $ \x -> if filterPF x then Seq.singleton x else Seq.empty


mapAllGatherEach2 :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapAllGatherEach2 = FL.fold
  (MR.mapReduceFold MR.uagMapAllGatherEachFold
                    g
                    monoidUnpackF
                    (MR.Assign assignPF)
                    (MR.foldAndRelabel reducePFold (\k m -> [(k, m)]))
  )

mapEach :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapEach = FL.fold
  (MR.mapReduceFold MR.uagMapEachFold
                    g
                    monoidUnpackF
                    (MR.Assign assignPF)
                    (MR.foldAndRelabel reducePFold (\k m -> [(k, m)]))
  )

mapAllGatherOnce :: Foldable g => g (Char, Int) -> [(Char, Double)]
mapAllGatherOnce = FL.fold
  (MR.mapReduceFold MR.uagMapAllGatherOnceFold
                    g
                    monoidUnpackF
                    (MR.Assign assignPF)
                    (MR.foldAndRelabel reducePFold (\k m -> [(k, m)]))
  )

benchOne dat = bgroup
  "Task 1, on (Char, Int) "
  [ bench "direct" $ nf direct dat
  , bench "copy steps" $ nf copySteps dat
  , bench "map-reduce-fold (mapAllGatherEach, filter with Maybe)"
    $ nf mapAllGatherEach dat
  , bench "map-reduce-fold (mapAllGatherEach parallel, filter with Maybe)"
    $ nf mapAllGatherEachP dat
  , bench "map-reduce-fold (mapAllGatherEach, filter with [])"
    $ nf mapAllGatherEach2 dat
  , bench "map-reduce-fold (mapEach, filter with [])" $ nf mapEach dat
  , bench "map-reduce-fold (mapAllGatherOnce, filter with [])"
    $ nf mapAllGatherOnce dat
  ]

-- a more complex task
createMapRows :: Int -> Seq.Seq (M.Map T.Text Int)
createMapRows n =
  let makeRow k = if even k
        then M.fromList [("A", k), ("B", k `mod` 47), ("C", k `mod` 13)]
        else M.fromList [("A", k), ("B", k `mod` 47)]
  in  Seq.unfoldr (\m -> if m > n then Nothing else Just (makeRow m, m + 1)) 0

-- unpack: if A and B and C are present, unpack to (A,B,C)
-- group by the value of "C"
-- compute the average of the sum of the values in A and B for each group
-- return Map Int Int with (C, <A+B>)
unpackMF :: M.Map T.Text Int -> Maybe (Int, Int, Int)
unpackMF m = do
  a <- M.lookup "A" m
  b <- M.lookup "B" m
  c <- M.lookup "C" m
  return (a, b, c)

assignMF :: (Int, Int, Int) -> (Int, (Int, Int))
assignMF (a, b, c) = (c, (a, b))

reduceMFold :: FL.Fold (Int, Int) Double
reduceMFold = FL.premap (\(x, y) -> realToFrac $ x + y) FL.mean


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
  (MR.basicListFold @Hashable
    (MR.Unpack unpackMF)
    (MR.Assign assignMF)
    (MR.foldAndRelabel reduceMFold (\k x -> [(k, x)]))
  )

basicListP :: Foldable g => g (M.Map T.Text Int) -> [(Int, Double)]
basicListP = FL.fold
  (MR.parBasicListHashableFold
    1000
    6
    (MR.Unpack unpackMF)
    (MR.Assign assignMF)
    (MR.foldAndRelabel reduceMFold (\k x -> [(k, x)]))
  )



benchTwo dat = bgroup
  "Task 2, on Map Text Int "
  [ bench "direct" $ nf directM dat
  , bench "map-reduce-fold (basicList)" $ nf basicList dat
  , bench "map-reduce-fold (basicList, parallel)" $ nf basicListP dat
  ]



main :: IO ()
main = defaultMain
  [benchOne $ createPairData 100000, benchTwo $ createMapRows 100000]
