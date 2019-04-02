{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes            #-}
import           Criterion.Main
import           Criterion


import           Control.MapReduce             as MR
import           Control.MapReduce.Engines.GroupBy
                                               as MRG
import           Data.Function                  ( on )
import           Data.Text                     as T
import           Data.List                     as L
import           Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(Identity)
                                                , runIdentity
                                                )
import           Data.Sequence                 as Seq
import           Data.Maybe                     ( catMaybes )
import           System.Random                  ( randomRs
                                                , newStdGen
                                                )

import qualified Data.HashMap.Lazy             as HML
import qualified Data.HashMap.Strict           as HMS
import qualified Data.Map                      as ML
import qualified Data.Map.Strict               as MS

createPairData :: Int -> IO [(Char, Int)]
createPairData n = do
  g <- newStdGen
  let randLabels = L.take n $ randomRs ('A', 'Z') g
      randInts   = L.take n $ randomRs (1, 100) g
  return $ L.zip randLabels randInts
--      makePair k = (toEnum $ fromEnum 'A' + k `mod` 26, k `mod` 31)
--  in  L.unfoldr (\m -> if m > n then Nothing else Just (makePair m, m + 1)) 0

promote :: (Char, Int) -> (Char, [Int])
promote (k, x) = (k, [x])

justSort :: [(Char, Int)] -> [(Char, Int)]
justSort = L.sortBy (compare `on` fst)

listViaStrictMap :: [(Char, Int)] -> [(Char, [Int])]
listViaStrictMap = MS.toList . MS.fromListWith (<>) . fmap promote
{-# INLINE listViaStrictMap #-}

listViaLazyMap :: [(Char, Int)] -> [(Char, [Int])]
listViaLazyMap = ML.toList . ML.fromListWith (<>) . fmap promote
{-# INLINE listViaLazyMap #-}

listViaStrictHashMap :: [(Char, Int)] -> [(Char, [Int])]
listViaStrictHashMap = HMS.toList . HMS.fromListWith (<>) . fmap promote
{-# INLINE listViaStrictHashMap #-}

listViaLazyHashMap :: [(Char, Int)] -> [(Char, [Int])]
listViaLazyHashMap = HML.toList . HML.fromListWith (<>) . fmap promote
{-# INLINE listViaLazyHashMap #-}

listViaTVL :: [(Char, Int)] -> [(Char, [Int])]
listViaTVL = MRG.groupByTVL
{-# INLINE listViaTVL #-}

listHandRolled :: [(Char, Int)] -> [(Char, [Int])]
listHandRolled = MRG.groupByHR
{-# INLINE listHandRolled #-}

rsNaiveInsert :: [(Char, Int)] -> [(Char, [Int])]
rsNaiveInsert = MRG.groupByNaiveInsert
{-# INLINE rsNaiveInsert #-}

rsNaiveBubble :: [(Char, Int)] -> [(Char, [Int])]
rsNaiveBubble = MRG.groupByNaiveBubble
{-# INLINE rsNaiveBubble #-}

rsNaiveInsert' :: [(Char, Int)] -> [(Char, [Int])]
rsNaiveInsert' = MRG.groupByNaiveInsert'
{-# INLINE rsNaiveInsert' #-}

rsNaiveBubble' :: [(Char, Int)] -> [(Char, [Int])]
rsNaiveBubble' = MRG.groupByNaiveBubble'
{-# INLINE rsNaiveBubble' #-}

rsInsert :: [(Char, Int)] -> [(Char, [Int])]
rsInsert = MRG.groupByInsert
{-# INLINE rsInsert #-}

rsBubble :: [(Char, Int)] -> [(Char, [Int])]
rsBubble = MRG.groupByBubble
{-# INLINE rsBubble #-}

rsInsert' :: [(Char, Int)] -> [(Char, [Int])]
rsInsert' = MRG.groupByInsert'
{-# INLINE rsInsert' #-}

rsBubble' :: [(Char, Int)] -> [(Char, [Int])]
rsBubble' = MRG.groupByBubble'
{-# INLINE rsBubble' #-}

rsNaiveInsert2 :: [(Char, Int)] -> [(Char, [Int])]
rsNaiveInsert2 = MRG.groupByNaiveInsert2
{-# INLINE rsNaiveInsert2 #-}


groupSum :: [(Char, [Int])] -> ML.Map Char Int
groupSum = ML.fromList . fmap (\(k, ln) -> (k, L.sum ln))

check reference toCheck = do
  let
    refGS = groupSum reference
    checkOne (name, gl) =
      let gs = groupSum gl
      in
        if refGS == gs
          then putStrLn (name ++ " good.")
          else putStrLn
            (name ++ " different!\n ref=\n" ++ show refGS ++ "\n" ++ show gs)
  mapM_ checkOne toCheck

benchMaps dat = bgroup
  (show (L.length dat) ++ " of [(Char, Int)]")
  [ bench "justSort" $ nf justSort dat
  , bench "listViaStrictMap" $ nf listViaStrictMap dat
  , bench "listViaLazyMap" $ nf listViaLazyMap dat
  , bench "listViaStrictHashMap" $ nf listViaStrictHashMap dat
  , bench "listViaLazyHashMap" $ nf listViaLazyHashMap dat
  , bench "TVLmerge" $ nf listViaTVL dat
  , bench "List.sort + hand-rolled merging" $ nf listHandRolled dat
  , bench "recursion-schemes, naiveInsert w/grouping" $ nf rsNaiveInsert dat
  , bench "recursion-schemes, naiveInsert (grouping swap)"
    $ nf rsNaiveInsert' dat
  , bench "recursion-schemes, naiveBubble w/grouping" $ nf rsNaiveBubble dat
  , bench "recursion-schemes, naiveBubble (grouping swap version)"
    $ nf rsNaiveBubble' dat
  , bench "recursion-schemes, insert (fold of grouping apo)" $ nf rsInsert dat
  , bench "recursion-schemes, bubble (unfold of grouping para)"
    $ nf rsBubble dat
  , bench "recursion-schemes, insert (fold of grouping apo, swop version)"
    $ nf rsInsert' dat
  , bench "recursion-schemes, bubble (unfold of grouping para, swop version)"
    $ nf rsBubble' dat
  , bench "recursion-schemes, naiveInsert w/grouping, internal x -> [x]"
    $ nf rsNaiveInsert2 dat
  ]

main :: IO ()
main = do
  dat <- createPairData 50000
  check
    (listViaStrictMap dat)
    (fmap
      (\(k, f) -> (k, f dat))
      [ ("lazy map"                 , listViaLazyMap)
      , ("strict hash map"          , listViaStrictHashMap)
      , ("lazy hash map"            , listViaLazyHashMap)
      , ("TVL general merge"        , listViaTVL)
      , ("List.sort + fold to group", listHandRolled)
      , ("recursion-schemes, naive insert + group", rsNaiveInsert)
      , ( "recursion-schemes, naive insert (grouping swap version)"
        , rsNaiveInsert'
        )
      , ("recursion-schemes, naive bubble + group", rsNaiveBubble)
      , ( "recursion-schemes, naive bubble (grouping swap version)"
        , rsNaiveBubble'
        )
      , ("recursion-schemes, insert (fold of grouping apo)"   , rsInsert)
      , ("recursion-schemes, bubble (unfold of grouping para)", rsBubble)
      , ( "recursion-schemes, insert (fold of grouping apo, swop version)"
        , rsInsert'
        )
      , ( "recursion-schemes, bubble (unfold of grouping para, swop version)"
        , rsBubble'
        )
      , ( "recursion-schemes, naive insert + group + internal x -> [x]"
        , rsNaiveInsert2
        )
      ]
    )
  putStrLn ""
  defaultMain [benchMaps dat]
