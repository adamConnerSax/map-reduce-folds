import qualified Control.MapReduce.Simple      as MR
import qualified Control.MapReduce.Engines.Streamly
                                               as MRS
import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M

unpack = MR.filterUnpack even -- filter, keeping even numbers
isMultipleOf3 x = (x `mod` 3) == 0

-- assign each number a key of True or False depending on whether it's a multiple of 3. Group the number itself.
assign = MR.assign isMultipleOf3 id

reduce = MR.foldAndLabel FL.sum M.singleton -- sum each group and then create a singleton Map for later combining

-- of the first n integers, compute the sum of even numbers which are mutliples of 3 
-- and the sum of all the even numbers which are not multiples of 3. 
mrFold :: FL.Fold Int (M.Map Bool Int)
mrFold = MRS.concatStreamFold
  $ MRS.streamlyEngine MRS.groupByHashableKey unpack assign reduce

result :: Int -> M.Map Bool Int
result n = FL.fold mrFold [1 .. n]

main = putStrLn $ show $ result 10

{- Output
fromList [(False,24),(True,6)]
-}
