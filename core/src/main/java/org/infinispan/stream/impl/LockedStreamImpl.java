package org.infinispan.stream.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.EntryWrapper;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.function.SerializablePredicate;

/**
 * Lock Stream implementation that locks a key using the {@link LockManager} before and after executing the various
 * code.
 * <p>
 * This implementation doesn't work properly when using an optimistic transactional cache. Care should be made to prevent
 * that usage if possible.
 * @author wburns
 * @since 9.0
 */
public class LockedStreamImpl<K, V> implements LockedStream<K, V> {
   final CacheStream<CacheEntry<K, V>> realStream;
   final Predicate<? super CacheEntry<K, V>> predicate;
   final long time;
   final TimeUnit unit;

   public LockedStreamImpl(CacheStream<CacheEntry<K, V>> realStream, long time, TimeUnit unit) {
      this.realStream = Objects.requireNonNull(realStream);
      this.predicate = null;
      if (time <= 0) {
         throw new IllegalArgumentException("time must be greater than 0");
      }
      this.time = time;
      this.unit = Objects.requireNonNull(unit);
   }

   LockedStreamImpl(CacheStream<CacheEntry<K, V>> realStream, Predicate<? super CacheEntry<K, V>> predicate,
         long time, TimeUnit unit) {
      this.realStream = realStream;
      this.predicate = predicate;
      this.time = time;
      this.unit = unit;
   }

   private LockedStream<K, V> newOrReuse(CacheStream<CacheEntry<K, V>> resultingStream) {
      if (resultingStream == realStream) {
         return this;
      } else {
         return newStream(resultingStream, predicate, time, unit);
      }
   }

   /**
    * Method to be overridden by a subclass so that chained methods return the correct implementation
    * @param realStream the underlying stream
    * @param predicate the predicate to use
    * @param time the time value to use
    * @param unit the new unit to use
    * @return the lock stream to return
    */
   LockedStreamImpl<K, V> newStream(CacheStream<CacheEntry<K, V>> realStream, Predicate<? super CacheEntry<K, V>> predicate,
         long time, TimeUnit unit) {
      return new LockedStreamImpl<>(realStream, predicate, time, unit);
   }

   @Override
   public LockedStream<K, V> filter(final Predicate<? super CacheEntry<K, V>> predicate) {
      Objects.nonNull(predicate);
      Predicate<? super CacheEntry<K, V>> usedPredicate;
      if (this.predicate != null) {
         usedPredicate = (SerializablePredicate<? super CacheEntry<K, V>>) e -> this.predicate.test(e) && predicate.test(e);
      } else {
         usedPredicate = predicate;
      }
      return newStream(realStream, usedPredicate, time, unit);
   }

   @Override
   public void forEach(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> biConsumer) {
      realStream.forEach(new CacheEntryConsumer<>(biConsumer, predicate));
   }

   @Override
   public <R> Map<K, R> invokeAll(BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R> biFunction) {
      Map<K, R> map = new HashMap<>();
      Iterator<KeyValuePair<K, R>> iterator =
            realStream.flatMap(new CacheEntryFunction<>(biFunction, predicate))
                      .iterator();
      iterator.forEachRemaining(e -> map.put(e.getKey(), e.getValue()));
      return map;
   }

   @Override
   public LockedStream<K, V> sequentialDistribution() {
      return newOrReuse(realStream.sequentialDistribution());
   }

   @Override
   public LockedStream<K, V> parallelDistribution() {
      return newOrReuse(realStream.parallelDistribution());
   }

   @Override
   public LockedStream<K, V> filterKeySegments(IntSet segments) {
      return newOrReuse(realStream.filterKeySegments(segments));
   }

   @Override
   public LockedStream<K, V> filterKeys(Set<?> keys) {
      return newOrReuse(realStream.filterKeys(keys));
   }

   @Override
   public LockedStream<K, V> distributedBatchSize(int batchSize) {
      return newOrReuse(realStream.distributedBatchSize(batchSize));
   }

   @Override
   public LockedStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("LockedStream doesn't support completion listener");
   }

   @Override
   public LockedStream<K, V> disableRehashAware() {
      return newOrReuse(realStream.disableRehashAware());
   }

   @Override
   public LockedStream timeout(long timeout, TimeUnit unit) {
      return newOrReuse(realStream.timeout(timeout, unit));
   }

   @Override
   public Iterator<CacheEntry<K, V>> iterator() {
      throw new UnsupportedOperationException("LockedStream doesn't support iterator");
   }

   @Override
   public Spliterator<CacheEntry<K, V>> spliterator() {
      throw new UnsupportedOperationException("LockedStream doesn't support spliterator");
   }

   @Override
   public boolean isParallel() {
      return realStream.isParallel();
   }

   @Override
   public LockedStream<K, V> sequential() {
      return newOrReuse(realStream.sequential());
   }

   @Override
   public LockedStream<K, V> parallel() {
      return newOrReuse(realStream.parallel());
   }

   @Override
   public LockedStream<K, V> unordered() {
      // This stream is always unordered
      return this;
   }

   @Override
   public LockedStream<K, V> onClose(Runnable closeHandler) {
      return newOrReuse(realStream.onClose(closeHandler));
   }

   @Override
   public void close() {
      realStream.close();
   }

   @Scope(Scopes.NONE)
   static abstract class LockHelper<K, V, R> {
      protected final Predicate<? super CacheEntry<K, V>> predicate;
      protected volatile transient LockManager lockManager;

      @Inject
      void init(LockManager lockManager) {
         this.lockManager = lockManager;
      }

      protected LockHelper(Predicate<? super CacheEntry<K, V>> predicate) {
         this.predicate = predicate;
      }

      @ProtoField(1)
      MarshallableObject<Predicate<? super CacheEntry<K, V>>> getPredicate() {
         return MarshallableObject.create(predicate);
      }

      R perform(Cache<K, V> cache, CacheEntry<K, V> entry) {
         K key = entry.getKey();
         lock(key);
         try {
            CacheEntry<K, V> rereadEntry = cache.getAdvancedCache().getCacheEntry(key);
            if (rereadEntry != null && (predicate == null || predicate.test(rereadEntry))) {
               Cache<K, V> cacheToUse = cache.getAdvancedCache().lockAs(key);
               return actualPerform(cacheToUse, rereadEntry);
            }
            return null;
         } finally {
            lockManager.unlock(key, key);
         }
      }

      protected abstract R actualPerform(Cache<K, V> cache, CacheEntry<K, V> entry);

      private void lock(K key) {
         KeyAwareLockPromise kalp = lockManager.lock(key, key, 10, TimeUnit.SECONDS);
         if (!kalp.isAvailable()) {
            try {
               ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
                  @Override
                  public boolean block() throws InterruptedException {
                     kalp.lock();
                     return true;
                  }

                  @Override
                  public boolean isReleasable() {
                     return kalp.isAvailable();
                  }
               });
            } catch (InterruptedException e) {
               throw new CacheException(e);
            } catch (TimeoutException e) {
               throw new CacheException("Could not acquire lock for key: " + key + " in 10 seconds");
            }
         }
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.STREAM_LOCKED_STREAM_CACHE_ENTRY_FUNCTION)
   @Scope(Scopes.NONE)
   public static class CacheEntryFunction<K, V, R> extends LockHelper<K, V, KeyValuePair<K, R>> implements Function<CacheEntry<K, V>, Stream<KeyValuePair<K, R>>> {
      private final BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R> biFunction;
      @Inject protected transient Cache<K, V> cache;

      protected CacheEntryFunction(BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R> biFunction,
            Predicate<? super CacheEntry<K, V>> predicate) {
         super(predicate);
         this.biFunction = biFunction;
      }

      @ProtoFactory
      CacheEntryFunction(MarshallableObject<Predicate<? super CacheEntry<K, V>>> predicate,
                         MarshallableObject<BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R>> biFunction) {
         this(MarshallableObject.unwrap(biFunction),  MarshallableObject.unwrap(predicate));
      }

      @ProtoField(2)
      MarshallableObject<BiFunction<Cache<K, V>, ? super CacheEntry<K, V>, R>> getBiFunction() {
         return MarshallableObject.create(biFunction);
      }

      @Override
      public Stream<KeyValuePair<K, R>> apply(CacheEntry<K, V> kvCacheEntry) {
         KeyValuePair<K, R> pair = perform(cache, kvCacheEntry);
         return pair != null ? Stream.of(pair) : Stream.empty();
      }

      @Override
      protected KeyValuePair<K, R> actualPerform(Cache<K, V> cache, CacheEntry<K, V> entry) {
         return new KeyValuePair<>(entry.getKey(), biFunction.apply(cache, entry));
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.STREAM_LOCKED_STREAM_CACHE_ENTRY_CONSUMER)
   public static class CacheEntryConsumer<K, V> extends LockHelper<K, V, Void> implements BiConsumer<Cache<K, V>, CacheEntry<K, V>> {
      private final BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer;

      private CacheEntryConsumer(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer,
            Predicate<? super CacheEntry<K, V>> predicate) {
         super(predicate);
         this.realConsumer = realConsumer;
      }

      @ProtoFactory
      CacheEntryConsumer(MarshallableObject<Predicate<? super CacheEntry<K, V>>> predicate,
                         MarshallableObject<BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>>> realConsumer) {
         this(MarshallableObject.unwrap(realConsumer),  MarshallableObject.unwrap(predicate));
      }

      @ProtoField(2)
      MarshallableObject<BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>>> getRealConsumer() {
         return MarshallableObject.create(realConsumer);
      }

      @Override
      public void accept(Cache<K, V> kvCache, CacheEntry<K, V> kvCacheEntry) {
         perform(kvCache, kvCacheEntry);
      }

      @Override
      protected Void actualPerform(Cache<K, V> cache, CacheEntry<K, V> entry) {
         realConsumer.accept(cache, new EntryWrapper<>(cache, entry));
         return null;
      }
   }
}
