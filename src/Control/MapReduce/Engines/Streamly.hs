{-# LANGUAGE CPP                   #-}
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
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE BangPatterns          #-}
{-# OPTIONS_GHC -O2 -fwarn-incomplete-patterns #-}
{-|
Module      : Control.MapReduce.Engines.Streams
Description : map-reduce-folds builders
Copyright   : (c) Adam Conner-Sax 2019
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

map-reduce engine (fold builder) using @Streamly@ streams as its intermediate and return type.

Notes:
1. These are polymorphic in the return stream type.  Thought the streams do have to be @serial@ when @groupBy@ is called
So you have to specify the stream type in the call or it has to be inferrable from the use of the result.

2. There is a concurrent engine here, one that uses Streamly's concurrency features to map over the stream.  I've not
been able to verify that this is faster on an appropriate task with appropriate runtime settings.
-}
module Control.MapReduce.Engines.Streamly
  (
    -- * Engines
    streamlyEngine
  , streamlyEngineM
  , concurrentStreamlyEngine

    -- * Streamly Combinators
  , toStreamlyFold
  , toStreamlyFoldM

    -- * Result Extraction
  , resultToList
  , concatStream
  , concatStreamFold
  , concatStreamFoldM
  , concatConcurrentStreamFold

    -- * @groupBy@ Functions
  , groupByHashableKey
  , groupByOrderedKey
#if MIN_VERSION_streamly(0,9,0)
  , groupByOrderedKeyIO
#endif
  , groupByHashableKeyST
  , groupByDiscriminatedKey

    -- * Re-Exports
#if MIN_VERSION_streamly(0,9,0)
  , Stream
#else
  , SerialT
  , WSerialT
  , AheadT
  , AsyncT
  , WAsyncT
  , ParallelT
  , MonadAsync
  , IsStream
#endif
  )
where

import qualified Control.MapReduce.Core        as MRC
import qualified Control.MapReduce.Engines     as MRE

import           Control.Arrow                  ( second )
import qualified Control.Foldl                 as FL
import           Control.Monad.ST              as ST
import qualified Data.Discrimination.Grouping  as DG
import qualified Data.Foldable                 as F
import           Data.Functor.Identity          ( Identity(runIdentity) )
import           Data.Hashable                  ( Hashable )
import qualified Data.HashMap.Strict           as HMS
import qualified Data.HashTable.Class          as HT
import qualified Data.HashTable.ST.Cuckoo      as HTC
import qualified Data.List.NonEmpty            as LNE
import qualified Data.Maybe                    as Maybe
import qualified Data.Map.Strict               as MS
import qualified Data.Sequence                 as Seq
#if MIN_VERSION_streamly(0,9,0)
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as S
import qualified Streamly.Data.Stream.Prelude as SP
import qualified Streamly.Data.StreamK as StreamK
import qualified Streamly.Data.Fold   as SF
import qualified Streamly.Data.Unfold   as SU
import qualified Streamly.Internal.Data.Fold   as SF
import           Control.Monad (join)
import           Control.Monad.IO.Class (MonadIO)
#elif MIN_VERSION_streamly(0,8,0)
import qualified Streamly.Prelude              as S
import qualified Streamly.Internal.Data.Fold   as SF
import           Streamly.Prelude              ( SerialT
                                                , WSerialT
                                                , AheadT
                                                , AsyncT
                                                , WAsyncT
                                                , ParallelT
                                                , MonadAsync
                                                , IsStream
                                                )
#else
import qualified Streamly.Prelude              as S
import qualified Streamly                      as S
import qualified Streamly.Internal.Data.Fold   as SF
import           Streamly                       ( SerialT
                                                , WSerialT
                                                , AheadT
                                                , AsyncT
                                                , WAsyncT
                                                , ParallelT
                                                , MonadAsync
                                                , IsStream
                                                )
--import qualified Streamly.Internal.Data.Parser.ParserK.Type as Streamly
#endif
#if MIN_VERSION_streamly(0,9,0)
-- | convert a Control.Foldl FoldM into a Streamly.Data.Fold fold
toStreamlyFoldM :: Functor m => FL.FoldM m a b -> SF.Fold m a b
toStreamlyFoldM (FL.FoldM step start done) = SF.Fold step' (SF.Partial <$> start) done where
  step' s a = SF.Partial <$> step s a

-- | convert a Control.Foldl Fold into a Streamly.Data.Fold fold
toStreamlyFold :: Monad m => FL.Fold a b -> SF.Fold m a b
toStreamlyFold (FL.Fold step start done) = SF.Fold step' (pure $ SF.Partial $ start) (pure . done) where
  step' s a = pure $ SF.Partial $ step s a

#elif MIN_VERSION_streamly(0,8,0)
fromEffect :: (Monad m, IsStream t) => m a -> t m a
fromEffect = S.fromEffect
{-# INLINE fromEffect #-}

-- | convert a Control.Foldl FoldM into a Streamly.Data.Fold fold
toStreamlyFoldM :: Functor m => FL.FoldM m a b -> SF.Fold m a b
toStreamlyFoldM (FL.FoldM step start done) = SF.mkFoldM step' (SF.Partial <$> start) done where
  step' s a = SF.Partial <$> step s a

-- | convert a Control.Foldl Fold into a Streamly.Data.Fold fold
toStreamlyFold :: Monad m => FL.Fold a b -> SF.Fold m a b
toStreamlyFold (FL.Fold step start done) = SF.mkFold step' (SF.Partial start) done where
  step' s a = SF.Partial $ step s a
#else
fromEffect :: (Monad m, IsStream t) => m a -> t m a
fromEffect = S.yieldM
{-# INLINE fromEffect #-}

-- | convert a Control.Foldl FoldM into a Streamly.Data.Fold fold
toStreamlyFoldM :: FL.FoldM m a b -> SF.Fold m a b
toStreamlyFoldM (FL.FoldM step start done) = SF.mkFold step start done

-- | convert a Control.Foldl Fold into a Streamly.Data.Fold fold
toStreamlyFold :: Monad m => FL.Fold a b -> SF.Fold m a b
toStreamlyFold (FL.Fold step start done) = SF.mkPure step start done
#endif

-- | unpack for streamly based map/reduce
#if MIN_VERSION_streamly(0,9,0)
unpackStreamK :: MRC.Unpack x y -> StreamK.StreamK Identity x -> S.Stream Identity y
unpackStreamK (MRC.Filter t) = S.filter t . StreamK.toStream
unpackStreamK (MRC.Unpack f) = S.concatMap (StreamK.toStream . StreamK.fromFoldable . f) . StreamK.toStream
--  S.concatMap (StreamK.toStream . StreamK.fromFoldable . f)
{-# INLINABLE unpackStreamK #-}

-- | effectful (monadic) unpack for streamly based map/reduce
unpackStreamKM :: (Monad m) => MRC.UnpackM m x y -> StreamK.StreamK m x -> S.Stream m y
unpackStreamKM (MRC.FilterM t) = S.filterM t . StreamK.toStream
unpackStreamKM (MRC.UnpackM f) = S.concatMapM (fmap (StreamK.toStream . StreamK.fromFoldable) . f) . StreamK.toStream
{-# INLINABLE unpackStreamKM #-}

-- | make a stream into an (effectful) @[]@
resultToList :: (Monad m) => S.Stream m a -> m [a]
resultToList = S.toList
{-# INLINEABLE resultToList #-}

-- | mappend all in a monoidal stream
concatStream :: (Monad m, Monoid a) => S.Stream m a -> m a
concatStream = S.fold (SF.foldl' (<>) mempty)
{-# INLINEABLE concatStream #-}

-- | mappend everything in a pure Streamly fold
concatStreamFold :: Monoid b => FL.Fold a (S.Stream Identity b) -> FL.Fold a b
concatStreamFold = fmap (runIdentity . concatStream)
{-# INLINEABLE concatStreamFold #-}

-- | mappend everything in an effectful Streamly fold.
concatStreamFoldM
  :: (Monad m, Monoid b) => FL.FoldM m a (S.Stream m b) -> FL.FoldM m a b
concatStreamFoldM = MRC.postMapM concatStream
{-# INLINEABLE concatStreamFoldM #-}

-- | mappend everything in a concurrent Streamly fold.
concatConcurrentStreamFold
  :: (Monad m, Monoid b) => FL.Fold a (S.Stream m b) -> FL.FoldM m a b
concatConcurrentStreamFold = concatStreamFoldM . FL.generalize
{-# INLINEABLE concatConcurrentStreamFold #-}

-- | map-reduce-fold builder returning a @SerialT Identity d@ result
streamlyEngine
  :: (Foldable g, Functor g)
  => (forall z . S.Stream Identity (k, z) -> S.Stream Identity (k, g z))
  -> MRE.MapReduceFold y k c (S.Stream Identity) x d
streamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (flip StreamK.cons)
  StreamK.nil
  ( fmap (\(k, lc) -> MRE.reduceFunction r k lc)
    . groupByKey
    . fmap a
    . unpackStreamK u
  )
{-# INLINABLE streamlyEngine #-}

-- | unpack for concurrent streamly based map/reduce
unpackConcurrentlyK
  :: (SP.MonadAsync m) => MRC.Unpack x y -> StreamK.StreamK m x -> S.Stream m y
unpackConcurrentlyK (MRC.Filter t) = S.filter t . StreamK.toStream
unpackConcurrentlyK (MRC.Unpack f) = S.concatMap ((StreamK.toStream . StreamK.fromFoldable) . f) . StreamK.toStream
{-# INLINABLE unpackConcurrentlyK #-}

-- | possibly (depending on chosen stream types) concurrent map-reduce-fold builder returning an @(Istream t, MonadAsync m) => t m d@ result
concurrentStreamlyEngine
  :: forall m g y k c x d
   . (SP.MonadAsync m, Foldable g, Functor g)
  => (forall z . S.Stream m (k, z) -> S.Stream m (k, g z))
  -> MRE.MapReduceFold y k c (S.Stream m) x d
concurrentStreamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a' -> (pure a') `StreamK.consM` s)
  StreamK.nil
  ( S.mapM (\(k, lc) -> return $ MRE.reduceFunction r k lc)
  . groupByKey
  . S.mapM (return . a)
  . unpackConcurrentlyK u
  )
{-# INLINABLE concurrentStreamlyEngine #-}


-- | effectful map-reduce-fold engine returning a (Istream t => t m d) result
-- The "MonadAsync" constraint here more or less requires us to run in IO, or something IO like.
streamlyEngineM
  :: (Monad m, SP.MonadAsync m, Traversable g)
  => (forall z . S.Stream m (k, z) -> S.Stream m (k, g z))
  -> MRE.MapReduceFoldM m y k c (S.Stream m) x d
streamlyEngineM groupByKey u (MRC.AssignM a) r =
  FL.generalize
    $ FL.Fold
        (flip StreamK.cons)
        StreamK.nil
        ( S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
        . groupByKey -- this requires a serial stream.
        . S.mapM a
        . unpackStreamKM u
        )
{-# INLINABLE streamlyEngineM #-}

toHashMap :: (Monad m, Eq k, Monoid a, Hashable k) => SF.Fold m (k, a) (HMS.HashMap k a)
toHashMap = SF.foldl' (\hm (k, a) -> HMS.insertWith (<>) k a hm) HMS.empty
{-# INLINEABLE toHashMap #-}

streamMeta :: Monad m => SF.Fold m a b -> SU.Unfold m b c -> S.Stream m a  -> S.Stream m c
streamMeta fld unfld = S.concatEffect . fmap (S.unfold unfld) . S.fold fld
{-# INLINEABLE streamMeta #-}

-- | Group streamly stream of @(k,c)@ by @hashable@ key.
groupByHashableKey
  :: (Monad m, Hashable k, Eq k)
  => S.Stream m (k, c)
  -> S.Stream m (k, Seq.Seq c)
groupByHashableKey = streamMeta toHashMap (SU.lmap HMS.toList $ SU.fromList) . fmap (second Seq.singleton)
{-# INLINABLE groupByHashableKey #-}

toMap :: (Monad m, Monoid a, Ord k) => SF.Fold m (k, a) (MS.Map k a)
toMap = SF.foldl' (\hm (k, a) -> MS.insertWith (<>) k a hm) MS.empty
{-# INLINEABLE toMap #-}

-- | Group streamly stream of @(k,c)@ by ordered key.
groupByOrderedKey
  :: (Monad m, Ord k) => S.Stream m (k, c) -> S.Stream m (k, Seq.Seq c)
groupByOrderedKey = --streamMeta (SF.toMap fst (SF.lmap snd toSeq)) (SU.lmap MS.toList $ SU.fromList)
  streamMeta toMap (SU.lmap MS.toList $ SU.fromList) . fmap (second Seq.singleton)
{-# INLINABLE groupByOrderedKey #-}

toSeq :: Monad m => SF.Fold m a (Seq.Seq a)
toSeq = SF.foldl' (\s a -> s <> Seq.singleton a) mempty
{-# INLINEABLE toSeq #-}

-- | Group streamly stream of @(k,c)@ by ordered key. Using toMapIO for mutable cells in the fold.
groupByOrderedKeyIO
  :: (Monad m, MonadIO m, Ord k) => S.Stream m (k, c) -> S.Stream m (k, Seq.Seq c)
groupByOrderedKeyIO = streamMeta (SF.toMapIO fst (SF.lmap snd toSeq)) (SU.lmap MS.toList $ SU.fromList)
{-# INLINABLE groupByOrderedKeyIO #-}

{-
toHashMapSeqST :: (Monad m, Eq k, Hashable k) => SF.Fold m (k, a) (ST.ST s (HTC.HashTable s k (Seq.Seq a)))
toHashMapSeqST = SF.foldl' (\hm (k, a) -> traverse (mutate k (f a)) hm) HTC.new where
  mutate k f hm = HTC.mutate hm k f
  f :: forall s . a -> Maybe (Seq.Seq a) -> Maybe (Seq.Seq a, ((HTC.HashTable s k (Seq.Seq a))))
  f a ht saM = case saM of
    Nothing -> (Seq.singleton a, ())
    Just sa -> (sa Seq.|> a, ())
{-# INLINEABLE toHashMapSeqST #-}
-}

-- | Group streamly stream of @(k,c)@ by @hashable@ key. Uses mutable hashtables running in the ST monad.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKeyST
  :: forall m k c . (Monad m, Hashable k, Eq k)
  => S.Stream m (k, c)
  -> S.Stream m (k, Seq.Seq c)
groupByHashableKeyST st = S.concatEffect $ fmap listToStream $ S.toList st
--  lkc <- fromEffect (S.toList st)
  where
    listToStream :: [(k, c)] -> S.Stream m (k, Seq.Seq c)
    listToStream l = S.fromList $ ST.runST $ listToList l
    listToList :: [(k, c)] -> ST.ST s [(k, Seq.Seq c)]
    listToList = join . fmap HT.toList . toHT
    toHT :: [(k, c)] -> ST.ST s (HTC.HashTable s k (Seq.Seq c))
    toHT l = (MRE.fromListWithHT @HTC.HashTable) (<>)
        $ fmap (second Seq.singleton) l
{-# INLINABLE groupByHashableKeyST #-}


-- | Group streamly stream of @(k,c)@ by key with instance of Grouping from <http://hackage.haskell.org/package/discrimination>.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByDiscriminatedKey
  :: (Monad m, DG.Grouping k)
  => S.Stream m (k, c)
  -> S.Stream m (k, Seq.Seq c)
groupByDiscriminatedKey s =
  S.concatEffect
  $ (StreamK.toStream . StreamK.fromFoldable . Maybe.catMaybes . fmap (fmap g . LNE.nonEmpty) . DG.groupWith fst)
  <$> S.toList s
  where
    g :: LNE.NonEmpty (k, c) -> (k, Seq.Seq c)
    g x = let k = fst (LNE.head x) in (k, F.fold $ fmap (Seq.singleton . snd) x)
{-# INLINABLE groupByDiscriminatedKey #-}
#else

unpackStream :: S.IsStream t => MRC.Unpack x y -> t Identity x -> t Identity y
unpackStream (MRC.Filter t) = S.filter t
unpackStream (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackStream #-}

-- | effectful (monadic) unpack for streamly based map/reduce
unpackStreamM :: (S.IsStream t, Monad m) => MRC.UnpackM m x y -> t m x -> t m y
unpackStreamM (MRC.FilterM t) = S.filterM t
unpackStreamM (MRC.UnpackM f) = S.concatMapM (fmap S.fromFoldable . f)
{-# INLINABLE unpackStreamM #-}

-- | make a stream into an (effectful) @[]@
resultToList :: (Monad m, S.IsStream t) => t m a -> m [a]
resultToList = S.toList . S.adapt

-- | mappend all in a monoidal stream
concatStream :: (Monad m, Monoid a) => S.SerialT m a -> m a
concatStream = S.foldl' (<>) mempty

-- | mappend everything in a pure Streamly fold
concatStreamFold :: Monoid b => FL.Fold a (S.SerialT Identity b) -> FL.Fold a b
concatStreamFold = fmap (runIdentity . concatStream)

-- | mappend everything in an effectful Streamly fold.
concatStreamFoldM
  :: (Monad m, Monoid b, S.IsStream t) => FL.FoldM m a (t m b) -> FL.FoldM m a b
concatStreamFoldM = MRC.postMapM (concatStream . S.adapt)

-- | mappend everything in a concurrent Streamly fold.
concatConcurrentStreamFold
  :: (Monad m, Monoid b, S.IsStream t) => FL.Fold a (t m b) -> FL.FoldM m a b
concatConcurrentStreamFold = concatStreamFoldM . FL.generalize

-- | map-reduce-fold builder returning a @SerialT Identity d@ result
streamlyEngine
  :: (Foldable g, Functor g)
  => (forall z . S.SerialT Identity (k, z) -> S.SerialT Identity (k, g z))
  -> MRE.MapReduceFold y k c (SerialT Identity) x d
streamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (flip S.cons)
  S.nil
  ( S.map (\(k, lc) -> MRE.reduceFunction r k lc)
  . groupByKey
  . S.map a
  . unpackStream u
  )
{-# INLINABLE streamlyEngine #-}

-- | unpack for concurrent streamly based map/reduce
unpackConcurrently
  :: (S.MonadAsync m, S.IsStream t) => MRC.Unpack x y -> t m x -> t m y
unpackConcurrently (MRC.Filter t) = S.filter t
unpackConcurrently (MRC.Unpack f) = S.concatMap (S.fromFoldable . f)
{-# INLINABLE unpackConcurrently #-}

-- | possibly (depending on chosen stream types) concurrent map-reduce-fold builder returning an @(Istream t, MonadAsync m) => t m d@ result
concurrentStreamlyEngine
  :: forall tIn tOut m g y k c x d
   . (S.IsStream tIn, S.IsStream tOut, S.MonadAsync m, Foldable g, Functor g)
  => (forall z . S.SerialT m (k, z) -> S.SerialT m (k, g z))
  -> MRE.MapReduceFold y k c (tOut m) x d
concurrentStreamlyEngine groupByKey u (MRC.Assign a) r = FL.Fold
  (\s a' -> (return a') `S.consM` s)
  S.nil
  ( S.mapM (\(k, lc) -> return $ MRE.reduceFunction r k lc)
  . S.adapt @SerialT @tOut -- make it concurrent for reducing
  . groupByKey
  . S.adapt @tIn @SerialT-- make it serial for grouping
  . S.mapM (return . a)
  . (S.|$) (unpackConcurrently u)
  )
{-# INLINABLE concurrentStreamlyEngine #-}


-- | effectful map-reduce-fold engine returning a (Istream t => t m d) result
-- The "MonadAsync" constraint here more or less requires us to run in IO, or something IO like.
streamlyEngineM
  :: (S.IsStream t, Monad m, S.MonadAsync m, Traversable g)
  => (forall z . SerialT m (k, z) -> SerialT m (k, g z))
  -> MRE.MapReduceFoldM m y k c (t m) x d
streamlyEngineM groupByKey u (MRC.AssignM a) r =
  FL.generalize
    $ fmap S.adapt
    $ FL.Fold
        (flip S.cons)
        S.nil
        ( S.mapM (\(k, lc) -> MRE.reduceFunctionM r k lc)
        . groupByKey -- this requires a serial stream.
        . S.mapM a
        . unpackStreamM u
        )
{-# INLINABLE streamlyEngineM #-}

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of @(k,c)@ by @hashable@ key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKey
  :: (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByHashableKey s = do
  lkc <- fromEffect (S.toList s)
  let hm = HMS.fromListWith (<>) $ fmap (second $ Seq.singleton) lkc
  HMS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByHashableKey #-}

-- TODO: Try using Streamly folds and Map.insertWith instead of toList and fromListWith.  Prolly the same.
-- | Group streamly stream of @(k,c)@ by ordered key.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByOrderedKey
  :: (Monad m, Ord k) => S.SerialT m (k, c) -> S.SerialT m (k, Seq.Seq c)
groupByOrderedKey s = do
  lkc <- fromEffect (S.toList s)
  let hm = MS.fromListWith (<>) $ fmap (second $ Seq.singleton) lkc
  MS.foldrWithKey (\k lc s' -> S.cons (k, lc) s') S.nil hm
{-# INLINABLE groupByOrderedKey #-}

-- | Group streamly stream of @(k,c)@ by @hashable@ key. Uses mutable hashtables running in the ST monad.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByHashableKeyST
  :: (Monad m, Hashable k, Eq k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByHashableKeyST st = do
  lkc <- fromEffect (S.toList st)
  ST.runST $ do
    hm <- (MRE.fromListWithHT @HTC.HashTable) (<>)
      $ fmap (second Seq.singleton) lkc
    HT.foldM (\s' (k, sc) -> return $ S.cons (k, sc) s') S.nil hm
{-# INLINABLE groupByHashableKeyST #-}


-- | Group streamly stream of @(k,c)@ by key with instance of Grouping from <http://hackage.haskell.org/package/discrimination>.
-- NB: this function uses the fact that @SerialT m@ is a monad
groupByDiscriminatedKey
  :: (Monad m, DG.Grouping k)
  => S.SerialT m (k, c)
  -> S.SerialT m (k, Seq.Seq c)
groupByDiscriminatedKey s = do
  lkc <- fromEffect (S.toList s)
  let g :: LNE.NonEmpty (k, c) -> (k, Seq.Seq c)
      g x = let k = fst (LNE.head x) in (k, F.fold $ fmap (Seq.singleton . snd) x)
  S.fromFoldable $ Maybe.catMaybes . fmap (fmap g . LNE.nonEmpty) $ DG.groupWith fst lkc
{-# INLINABLE groupByDiscriminatedKey #-}
#endif
