{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE Rank2Types #-}
module InspectionTests where

--import           Hedgehog
--import qualified Hedgehog.Gen                  as Gen
--import qualified Hedgehog.Range                as Range

import qualified Test.Inspection               as T

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
direct :: Foldable f => f Int -> M.Map Bool Int
direct = M.fromListWith (+) . fmap (\x -> (x `mod` 3 == 0, x)) . L.filter even . FL.fold FL.list
{-# INLINE direct #-}

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

toInspect :: (forall f. Foldable f => f Int -> M.Map Bool Int) -> Int -> M.Map Bool Int
toInspect g n = g [0..n]

directTI = toInspect direct
--mrFoldListTI :: Foldable f => Int ->  M.Map Bool Int
mrFoldListTI = toInspect (FL.fold mrFoldList)

T.inspect $ 'directTI `T.hasNoType` ''[]
T.inspect $ 'mrFoldListTI `T.hasNoType` ''[] 

main :: IO ()
main = return ()
