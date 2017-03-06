package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Lock Stream implementation that locks a key using the {@link LockManager} before and after executing the various
 * code.
 * <p>
 * This implementation doesn't work properly when using an optimistic transactional cache. Care should be made to prevent
 * that usage if possible.
 * @author wburns
 * @since 9.0
 */
public class LockStreamImpl<K, V> implements LockedStream<K, V> {
   private final CacheStream<CacheEntry<K, V>> realStream;

   public static <K, V> LockStreamImpl<K, V> newStream(CacheStream<CacheEntry<K, V>> realStream) {
      return new LockStreamImpl<>(realStream
               .map(StreamMarshalling.entryToKeyFunction())
               .peek(new LockConsumer<>())
               .map(StreamMarshalling.<K, V>keyToEntryFunction())
               .filter(StreamMarshalling.nonNullPredicate()));
   }

   private LockStreamImpl(CacheStream<CacheEntry<K, V>> realStream) {
      this.realStream = realStream;
   }

   private LockedStream<K, V> newOrReuse(CacheStream<CacheEntry<K, V>> resultingStream) {
      if (resultingStream == realStream) {
         return this;
      } else {
         return new LockStreamImpl<>(resultingStream);
      }
   }

   @Override
   public LockedStream<K, V> filter(Predicate<? super CacheEntry<K, V>> predicate) {
      return newOrReuse(realStream.filter(predicate));
   }

   @Override
   public void forEach(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> biConsumer) {
      realStream.forEach(new CacheEntryConsumer<>(biConsumer));
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
   public BaseCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("LockStream doesn't support completion listener");
   }

   @Override
   public BaseCacheStream disableRehashAware() {
      return newOrReuse(realStream.disableRehashAware());
   }

   @Override
   public BaseCacheStream timeout(long timeout, TimeUnit unit) {
      return newOrReuse(realStream.timeout(timeout, unit));
   }

   @Override
   public Iterator<CacheEntry<K, V>> iterator() {
      return realStream.iterator();
   }

   @Override
   public Spliterator<CacheEntry<K, V>> spliterator() {
      return realStream.spliterator();
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

   @SerializeWith(value = LockConsumer.LockConsumerExternalizer.class)
   private static class LockConsumer<K> implements Consumer<K> {
      private transient LockManager lockManager;

      @Override
      public void accept(K key) {
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

      public static final class LockConsumerExternalizer implements Externalizer<LockConsumer> {
         @Override
         public void writeObject(ObjectOutput output, LockConsumer object) throws IOException { }

         @Override
         public LockConsumer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new LockConsumer();
         }
      }
   }

   @SerializeWith(value = LockConsumer.LockConsumerExternalizer.class)
   private static class CacheEntryConsumer<K, V> implements BiConsumer<Cache<K, V>, CacheEntry<K, V>>, Serializable {
      private final BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer;
      private transient LockManager lockManager;

      private CacheEntryConsumer(BiConsumer<Cache<K, V>, ? super CacheEntry<K, V>> realConsumer) {
         this.realConsumer = realConsumer;
      }

      @Override
      public void accept(Cache<K, V> kvCache, CacheEntry<K, V> kvCacheEntry) {
         K key = kvCacheEntry.getKey();
         try {
            // Pass the Cache with the owner set to our key so they can write and also it won't unlock that key
            realConsumer.accept(kvCache.getAdvancedCache().lockAs(key), kvCacheEntry);
         } finally {
            lockManager.unlock(key, key);
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
         }

         @Override
         public CacheEntryConsumer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CacheEntryConsumer((BiConsumer) input.readObject());
         }
      }
   }
}
