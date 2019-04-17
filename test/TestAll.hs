
{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Hedgehog

import           Test1                          ( tests )

main :: IO Bool
main = checkParallel tests
