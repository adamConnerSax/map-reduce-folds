{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
import           Criterion.Main
import           Criterion
import qualified Weigh                         as W

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

toTry :: [(String, [(Char, Int)] -> [(Char, [Int])])]
toTry =
  [ ( "strict map"
    , listViaStrictMap
    )
{-    
    , ("listViaLazyMap: lazy map"                 , listViaLazyMap)
    , ("listViaStrictMap: strict hash map"          , listViaStrictHashMap)
    , ("listViaLazyHashMap: lazy hash map"            , listViaLazyHashMap)
    , ("listViaTVL: TVL general merge"        , MRG.groupByTVL)
    , ("MRG.groupByHR: List.sort + fold to group", MRG.groupByHR)
  , ( "MRG.groupByNaiveInsert: recursion-schemes, naive insert + group"
    , MRG.groupByNaiveInsert
    )
  , ( "MRG.groupByNaiveBubble: recursion-schemes, naive bubble + group"
    , MRG.groupByNaiveBubble
    )
  , ( "MRG.groupByNaiveInsert': recursion-schemes, naive insert (grouping swap version)"
    , MRG.groupByNaiveInsert'
    )
-}
  , ( "MRG.groupByNaiveInsert: recursion-schemes, swap naive insert"
    , MRG.groupByNaiveInsert'
    )
  , ( "MRG.groupByNaiveBubble: recursion-schemes, swap naive bubble"
    , MRG.groupByNaiveBubble'
    )
  , ( "MRG.groupByNaiveInsertY: yaya, swap naive insert"
    , MRG.groupByNaiveInsertY
    )
  , ( "MRG.groupByNaiveBubbleY: yaya, swap naive bubble"
    , MRG.groupByNaiveBubbleY
    )
{-
  , ( "unDList . MRG.groupByNaiveBubble': recursion-schemes, naive bubble (grouping swap version, DList)"
    , unDList . MRG.groupByNaiveBubble'
    )
-}
  , ( "MRG.groupByInsert: recursion-schemes, insert (fold of grouping apo)"
    , MRG.groupByInsert
    )
  , ( "MRG.groupByBubble: recursion-schemes, bubble (unfold of grouping para)"
    , MRG.groupByBubble
    )
{-  
    , ( "MRG.groupByInsert': recursion-schemes, insert (fold of grouping apo, swop version)"
      , MTG.groupByInsert'
      )

  , ( "MRG.groupByBubble': recursion-schemes, bubble (unfold of grouping para, swop version)"
    , MRG.groupByBubble'
    )
-}
  , ( "MRG.groupByTree1: recursion-schemes, hylo (grouping unfold to Tree, fold to list)"
    , MRG.groupByTree1
    )
  , ( "MRG.groupByTree2: recursion-schemes, hylo (unfold to Tree, merge back up)"
    , MRG.groupByTree2
    )
{-      
    , ( "MRG.groupByNaiveInsert2: recursion-schemes, naive insert + group + internal x -> [x]"
      , MRG.groupByNaiveInsert2
      )
-}
  ]

benchAll dat toTry = defaultMain
  [ bgroup (show (L.length dat) ++ " of [(Char, Int)]")
           (fmap (\(n, f) -> (bench n $ nf f dat)) toTry)
  ]

checkAll dat toTry =
  check (listViaStrictMap dat) (fmap (\(k, f) -> (k, f dat)) toTry)

{- This is hanging...
weighAll dat toTry = W.mainWith $ mapM_ (\(n, f) -> W.func n f dat) toTry
-}

main :: IO ()
main = do
  dat <- createPairData 50000
  checkAll dat toTry
  putStrLn ""
  benchAll dat toTry
