{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.TypeLevel
Description : type-level gadgetry to manage the return type of the map-reduce folds. 
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental
-}
module Control.MapReduce.TypeLevel
  (
  -- * Type-Level gadgetry for polymorphic (in the Maybe (Type->Type) parameter) assemblers
    MOR
  , MORE
--  , MORC
  )
where

import           Data.Functor.Identity          ( Identity(Identity) )
import           Data.Kind                      ( Type
                                                , Constraint
                                                )
import           Data.Proxy                     ( Proxy(..) )
import           GHC.TypeLits                   ( TypeError
                                                , ErrorMessage(Text)
                                                )
import           Data.Type.Equality             ( (:~:) (Refl)
                                                )
import           Control.Monad                  (join)

-- some type-level machinery for managing monadic/non-monadic combinations

-- | compute the correct return type for a function taking two steps either or both of which may be monadic.
-- If both are monadic, the monads must be the same. This is a sort of "or", hence the name
type family MOR
  (m :: Type -> Type)
  (mm :: Maybe m)
  (mn :: Maybe m) :: Maybe m where
  MOR m 'Nothing 'Nothing = 'Nothing
  MOR m 'Nothing ('Just m) = 'Just m
  MOR m ('Just m) 'Nothing = 'Just m
  MOR m ('Just m) ('Just m) = 'Just m

-- | type level mapping from @Maybe (Type -> Type)@ to a result type for functions
type family WrapMaybe (mm :: Maybe (Type -> Type)) (a :: Type) :: Type where
  WrapMaybe 'Nothing a = a
  WrapMaybe ('Just m) a = m a


-- proofs of the properties of MOR

class MORable (m :: Type -> Type) (mm :: Maybe (Type -> Type)) (mn :: (Maybe (Type -> Type)))

instance MORable m 'Nothing 'Nothing
instance Monad m => MORable m 'Nothing ('Just m)
instance Monad m => MORable m ('Just m) 'Nothing
instance Monad m => MORable m ('Just m) ('Just m)

-- We need a value-level rep so we can do case analysis
data MonadMaybeS (m :: Type -> Type) (mm :: Maybe (Type -> Type)) where
  NothingS :: MonadMaybeS m 'Nothing
  JustS :: MonadMaybeS m ('Just m)

class ToMonadMaybeS (m :: Type -> Type) (mm :: Maybe (Type -> Type)) where
  toMonadMaybeS :: MonadMaybeS m mm

instance ToMonadMaybeS m 'Nothing where
  toMonadMaybeS = NothingS

instance ToMonadMaybeS m ('Just m) where
  toMonadMaybeS = JustS -- @('Just m)

pMORCommutes :: forall m x y. MonadMaybeS m x -> MonadMaybeS m y -> MOR m x y :~: MOR m y x
pMORCommutes NothingS NothingS = Refl
pMORCommutes JustS JustS = Refl
pMORCommutes NothingS JustS = pMORCommutes (toMonadMaybeS @m @(MOR m x y)) (toMonadMaybeS @m @(MOR m y x))
pMORCommutes JustS NothingS = pMORCommutes (toMonadMaybeS @m @(MOR m x y)) (toMonadMaybeS @m @(MOR m y x))
--pMORCommutes NothingS ('Just m) = pMORCommutes (toMonadMaybe @(MOR 'NothingS ('Just m)) (toMonadMaybe @(MOR)

data F (mm :: Maybe (Type -> Type)) a b where
  PlainF :: (a -> b) -> F 'Nothing a b
  MonadicF :: Monad m => (a -> m b) -> F ('Just m) a b

testCommute :: MORable m mm mn => F mm a b -> F mn b c -> F (MOR m mm mn) a c
testCommute f1 f2 = case f1 of
  PlainF g1 -> case f2 of
    PlainF g2 -> PlainF $ g2 . g1
    MonadicF g2 ->  MonadicF $ g2 . g1 
  MonadicF g1 -> case f2 of
    PlainF g2 -> MonadicF $ fmap g2 . g1
    MonadicF g2 -> MonadicF $ join . fmap g2 . g1

-- | compute the correct return type for a function taking three steps either or both of which may be monadic.  Three is "more" than two...
type family MORE (mm :: Maybe (Type -> Type)) (mn :: Maybe (Type -> Type)) (mp :: Maybe (Type -> Type)) :: Maybe (Type -> Type) where
  MORE 'Nothing 'Nothing 'Nothing = 'Nothing
  MORE 'Nothing 'Nothing ('Just m) = ('Just m)
  MORE 'Nothing ('Just m) 'Nothing = ('Just m)
  MORE ('Just m) 'Nothing 'Nothing = ('Just m)
  MORE ('Just m) ('Just m) 'Nothing = ('Just m)
  MORE ('Just m) 'Nothing ('Just m) = ('Just m)
  MORE 'Nothing ('Just m) ('Just m) = ('Just m)
  MORE _        _         _         = TypeError ('Text "different monads in MORE.  Likely a different monad in an assign, unpack or reduce step in same map-reduce.")
{-
-- | Group the constraints required to use MOR.
type MORC mn mm = ( CorrectStep mn (MOR mn mm)
                  , CorrectStep mm (MOR mn mm))

class CorrectStep (mn :: Maybe (Type->Type)) (mm :: Maybe (Type -> Type)) where
  correctUnpack :: Unpack mn g x y -> Unpack mm g x y
  correctAssign :: Assign mn k y c -> Assign mm k y c
  correctMapStep :: MapStep mn x q -> MapStep mm x q
  correctMapGather :: MapGather mn x ec gt k c d -> MapGather mm x ec gt k c d
  correctReduce :: Reduce mn k h x e -> Reduce mm k h x e

instance CorrectStep 'Nothing 'Nothing where
  correctUnpack = id
  correctAssign = id
  correctMapStep = id
  correctMapGather = id
  correctReduce = id

instance CorrectStep ('Just m) ('Just m) where
  correctUnpack = id
  correctAssign = id
  correctMapStep = id
  correctMapGather = id
  correctReduce = id

instance Monad m => CorrectStep 'Nothing ('Just m) where
  correctUnpack = generalizeUnpack
  correctAssign = generalizeAssign
  correctMapStep = generalizeMapStep
  correctMapGather = generalizeMapGather
  correctReduce = generalizeReduce

-}
