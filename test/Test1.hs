{-# LANGUAGE OverloadedStrings #-}
module Test1
  ( tests
  )
where

import           Hedgehog
import qualified Hedgehog.Gen                  as Gen
import qualified Hedgehog.Range                as Range

import qualified Data.Map                      as M
import qualified Data.List                     as L
import qualified Data.Foldable                 as F
import qualified Control.Foldl                 as FL

import qualified Control.MapReduce.Simple      as MR
import qualified Control.MapReduce.Engines.List
                                               as MRL
import qualified Control.MapReduce.Engines.Vector
                                               as MRV
import qualified Control.MapReduce.Engines.Streamly
                                               as MRSY
import qualified Control.MapReduce.Engines.Streaming
                                               as MRSG

-- for reference
direct :: [Int] -> M.Map Bool Int
direct = M.fromListWith (+) . fmap (\x -> (x `mod` 3 == 0, x)) . L.filter even

unpack = MR.filterUnpack even -- filter, keeping even numbers

-- assign each number a key of True or False depending on whether it's a multiple of 3. Group the number itself.
isMultipleOf3 x = (x `mod` 3) == 0
assign = MR.assign isMultipleOf3 id

reduce = MR.foldAndLabel FL.sum M.singleton -- sum each group and then create a singleton Map for later combining

mrFoldList :: FL.Fold Int (M.Map Bool Int)
mrFoldList =
  fmap F.fold $ MRL.listEngine MRL.groupByHashableKey unpack assign reduce

mrFoldVector :: FL.Fold Int (M.Map Bool Int)
mrFoldVector =
  fmap F.fold $ MRV.vectorEngine MRV.groupByHashableKey unpack assign reduce

mrFoldStreamly :: FL.Fold Int (M.Map Bool Int)
mrFoldStreamly = MRSY.concatStreamFold
  $ MRSY.streamlyEngine MRSY.groupByHashableKey unpack assign reduce

mrFoldStreaming :: FL.Fold Int (M.Map Bool Int)
mrFoldStreaming = MRSG.concatStreamFold
  $ MRSG.streamingEngine MRSG.groupByHashableKey unpack assign reduce

prop_mrSame :: FL.Fold Int (M.Map Bool Int) -> Property
prop_mrSame mrF = property $ do
  xs <- forAll $ Gen.list (Range.linear 0 100) $ Gen.int (Range.linear 0 10000)
  direct xs === FL.fold mrF xs

tests :: Group
tests = Group
  "Test 1"
  [ ("[] Engine"       , prop_mrSame mrFoldList)
  , ("Vector Engine"   , prop_mrSame mrFoldVector)
  , ("Streamly Engine" , prop_mrSame mrFoldStreamly)
  , ("Streaming Engine", prop_mrSame mrFoldStreaming)
  ]
