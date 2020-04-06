package org.infinispan.persistence.support;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.util.concurrent.CompletionStages;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public class SegmentedAdvancedLoadWriteStoreAdapter<K, V> implements SegmentedAdvancedLoadWriteStore<K, V> {
   private final CacheLoader<K, V> cacheLoader;
   private final CacheWriter<K, V> cacheWriter;

   private KeyPartitioner keyPartitioner;

   public SegmentedAdvancedLoadWriteStoreAdapter(CacheLoader<K, V> cacheLoader) {
      this.cacheLoader = Objects.requireNonNull(cacheLoader);
      this.cacheWriter = cacheLoader instanceof CacheWriter ? (CacheWriter) cacheLoader : null;
   }

   public SegmentedAdvancedLoadWriteStoreAdapter(CacheWriter<K, V> cacheWriter) {
      this.cacheWriter = Objects.requireNonNull(cacheWriter);
      this.cacheLoader = cacheWriter instanceof CacheLoader ? (CacheLoader) cacheWriter : null;
   }

   @Override
   public void init(InitializationContext ctx) {
      keyPartitioner = ctx.getKeyPartitioner();
      if (cacheLoader != null) {
         cacheLoader.init(ctx);
      } else {
         cacheWriter.init(ctx);
      }
   }

   @Override
   public void start() {
      if (cacheLoader != null) {
         cacheLoader.start();
      } else {
         cacheWriter.start();
      }
   }

   @Override
   public void stop() {
      if (cacheLoader != null) {
         cacheLoader.start();
      } else {
         cacheWriter.start();
      }
   }

   @Override
   public void destroy() {
      // ExternalStore is both loader and writer so can just check one
      if (cacheLoader instanceof ExternalStore) {
         ((ExternalStore<K, V>) cacheLoader).destroy();
      } else {
         stop();
      }
   }

   @Override
   public boolean isAvailable() {
      if (cacheLoader != null) {
         return cacheLoader.isAvailable();
      } else {
         return cacheWriter.isAvailable();
      }
   }

   @Override
   public MarshallableEntry<K, V> loadEntry(Object key) {
      return cacheLoader.loadEntry(key);
   }

   @Override
   public MarshallableEntry<K, V> get(int segment, Object key) {
      return loadEntry(key);
   }

   @Override
   public boolean contains(Object key) {
      return cacheLoader.contains(key);
   }

   @Override
   public boolean contains(int segment, Object key) {
      return contains(key);
   }

   @Override
   public void write(MarshallableEntry<? extends K, ? extends V> entry) {
      cacheWriter.write(entry);
   }

   @Override
   public void write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      write(entry);
   }

   @Override
   public boolean delete(Object key) {
      return false;
   }

   @Override
   public boolean delete(int segment, Object key) {
      return delete(key);
   }

   @Override
   public int size() {
      if (cacheLoader instanceof AdvancedCacheLoader) {
         return ((AdvancedCacheLoader<K, V>) cacheLoader).size();
      }
      return 0;
   }

   @Override
   public int size(IntSet segments) {
      return CompletionStages.join(Flowable.fromPublisher(publishKeys(segments, null))
            .count().toCompletionStage()).intValue();
   }

   @Override
   public Publisher<K> publishKeys(Predicate<? super K> filter) {
      if (cacheLoader instanceof AdvancedCacheLoader) {
         return ((AdvancedCacheLoader<K, V>) cacheLoader).publishKeys(filter);
      }
      return Flowable.empty();
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return publishKeys(PersistenceUtil.combinePredicate(segments, keyPartitioner, filter));
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      if (cacheLoader instanceof AdvancedCacheLoader) {
         return ((AdvancedCacheLoader<K, V>) cacheLoader).entryPublisher(filter, fetchValue, fetchMetadata);
      }
      return Flowable.empty();
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return entryPublisher(PersistenceUtil.combinePredicate(segments, keyPartitioner, filter), fetchValue, fetchMetadata);
   }

   @Override
   public void clear() {

   }

   @Override
   public void clear(IntSet segments) {

   }

   @Override
   public void addSegments(IntSet segments) {

   }

   @Override
   public void removeSegments(IntSet segments) {
      clear(segments);
   }

   @Override
   public void purge(Executor executor, ExpirationPurgeListener<K, V> listener) {

   }

   @Override
   public void purge(Executor threadPool, PurgeListener<? super K> listener) {

   }

   @Override
   public CompletionStage<Void> bulkUpdate(Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
      return null;
   }

   @Override
   public void deleteBatch(Iterable<Object> keys) {

   }
}
