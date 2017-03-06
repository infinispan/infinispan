package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.EntryWrapper;
import org.infinispan.util.concurrent.TimeoutException;
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
   public BaseCacheStream sequentialDistribution() {
      return newOrReuse(realStream.sequentialDistribution());
   }

   @Override
   public BaseCacheStream parallelDistribution() {
      return newOrReuse(realStream.parallelDistribution());
   }

   @Override
   public BaseCacheStream filterKeySegments(Set<Integer> segments) {
      return newOrReuse(realStream.filterKeySegments(segments));
   }

   @Override
   public BaseCacheStream filterKeys(Set<?> keys) {
      return newOrReuse(realStream.filterKeys(keys));
   }

   @Override
   public BaseCacheStream distributedBatchSize(int batchSize) {
      return newOrReuse(realStream.distributedBatchSize(batchSize));
   }

   @Override
   public LockedStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("LockedStream doesn't support completion listener");
   }

   @Override
   public BaseCacheStream disableRehashAware() {
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

   @SerializeWith(value = CacheEntryConsumer.CacheEntryConsumerExternalizer.class)
   private static class CacheEntryConsumer<K, V> implements BiConsumer<Cache<K, V>, CacheEntry<K, V>>, Serializable {
      private final BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer;
      private final Predicate<? super CacheEntry<K, V>> predicate;
      private transient LockManager lockManager;

      private CacheEntryConsumer(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer,
            Predicate<? super CacheEntry<K, V>> predicate) {
         this.realConsumer = realConsumer;
         this.predicate = predicate;
      }

      @Override
      public void accept(Cache<K, V> cache, CacheEntry<K, V> entry) {
         K key = entry.getKey();
         lock(key);
         try {
            CacheEntry<K, V> rereadEntry = cache.getAdvancedCache().getCacheEntry(key);
            if (rereadEntry != null && (predicate == null || predicate.test(rereadEntry))) {
               Cache<K, V> cacheToUse = cache.getAdvancedCache().lockAs(key);
               // Pass the Cache with the owner set to our key so they can write and also it won't unlock that key
               realConsumer.accept(cacheToUse, new EntryWrapper<>(cacheToUse, entry));
            }
         } finally {
            lockManager.unlock(key, key);
         }
      }

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

      @Inject
      public void inject(LockManager manager) {
         this.lockManager = manager;
      }

      public static final class CacheEntryConsumerExternalizer implements Externalizer<CacheEntryConsumer> {
         @Override
         public void writeObject(ObjectOutput output, CacheEntryConsumer object) throws IOException {
            output.writeObject(object.realConsumer);
            output.writeObject(object.predicate);
         }

         @Override
         public CacheEntryConsumer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CacheEntryConsumer((BiConsumer) input.readObject(), (Predicate) input.readObject());
         }
      }
   }
}
