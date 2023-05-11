package org.infinispan.persistence.support;

import java.util.PrimitiveIterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.concurrent.CompletionStages;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Segmented store that creates multiple inner stores for each segment. This is used by stores that are not segmented
 * but have a configuration that implements {@link AbstractSegmentedStoreConfiguration}.
 * @author wburns
 * @since 9.4
 */
public class ComposedSegmentedLoadWriteStore<K, V, T extends AbstractSegmentedStoreConfiguration> extends AbstractSegmentedAdvancedLoadWriteStore<K, V> {
   private final AbstractSegmentedStoreConfiguration<T> configuration;
   Cache<K, V> cache;
   KeyPartitioner keyPartitioner;
   InitializationContext ctx;
   boolean shouldStopSegments;

   AtomicReferenceArray<AdvancedLoadWriteStore<K, V>> stores;

   public ComposedSegmentedLoadWriteStore(AbstractSegmentedStoreConfiguration<T> configuration) {
      this.configuration = configuration;
   }

   @Override
   public ToIntFunction<Object> getKeyMapper() {
      return keyPartitioner;
   }

   @Override
   public MarshallableEntry<K, V> get(int segment, Object key) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      if (store != null) {
         return store.loadEntry(key);
      }
      return null;
   }

   @Override
   public boolean contains(int segment, Object key) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      return store != null && store.contains(key);
   }

   @Override
   public void write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      if (store != null) {
         store.write(entry);
      }
   }

   @Override
   public boolean delete(int segment, Object key) {
      AdvancedLoadWriteStore<K, V> store = stores.get(segment);
      return store != null && store.delete(key);
   }

   @Override
   public int size(IntSet segments) {
      int size = 0;
      PrimitiveIterator.OfInt segmentIterator = segments.iterator();
      while (segmentIterator.hasNext()) {
         int segment = segmentIterator.nextInt();
         AdvancedLoadWriteStore<K, V> store = stores.get(segment);
         if (store != null) {
            size += store.size();
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public int size() {
      int size = 0;
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> store = stores.get(i);
         if (store != null) {
            size += store.size();
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      IntFunction<Publisher<K>> publisherFunction = i -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            return alws.publishKeys(filter);
         }
         return Flowable.empty();
      };
      if (segments.size() == 1) {
         return publisherFunction.apply(segments.iterator().nextInt());
      }
      return Flowable.fromStream(segments.intStream().mapToObj(publisherFunction))
            .concatMap(RxJavaInterop.identityFunction());
   }

   @Override
   public Publisher<K> publishKeys(Predicate<? super K> filter) {
      return publishKeys(IntSets.immutableRangeSet(stores.length()), filter);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      IntFunction<Publisher<MarshallableEntry<K, V>>> publisherFunction = i -> {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            return alws.entryPublisher(filter, fetchValue, fetchMetadata);
         }
         return Flowable.empty();
      };
      if (segments.size() == 1) {
         return publisherFunction.apply(segments.iterator().nextInt());
      }
      return Flowable.fromStream(segments.intStream().mapToObj(publisherFunction))
            .concatMap(RxJavaInterop.identityFunction());
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> entryPublisher(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return entryPublisher(IntSets.immutableRangeSet(stores.length()), filter, fetchValue, fetchMetadata);
   }

   @Override
   public void clear() {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws != null) {
            alws.clear();
         }
      }
   }

   @Override
   public void purge(Executor executor, ExpirationPurgeListener<K, V> listener) {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore<K, V> alws = stores.get(i);
         if (alws instanceof AdvancedCacheExpirationWriter) {
            ((AdvancedCacheExpirationWriter) alws).purge(executor, listener);
         } else if (alws != null) {
            alws.purge(executor, listener);
         }
      }
   }

   @Override
   public void clear(IntSet segments) {
      for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
         AdvancedLoadWriteStore<K, V> alws = stores.get(segmentIterator.nextInt());
         if (alws != null) {
            alws.clear();
         }
      }
   }

   @Override
   public void deleteBatch(Iterable<Object> keys) {
      CompletionStage<Void> stage = Flowable.fromIterable(keys)
            // Separate out batches by segment
            .groupBy(keyPartitioner::getSegment)
            .flatMap(groupedFlowable ->
                  groupedFlowable
                        .buffer(configuration.maxBatchSize())
                        .doOnNext(batch -> stores.get(groupedFlowable.getKey()).deleteBatch(batch))
                  , stores.length())
            .ignoreElements()
            .toCompletionStage(null);
      CompletionStages.join(stage);
   }

   @Override
   public CompletionStage<Void> bulkUpdate(Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
      return Flowable.fromPublisher(publisher)
            .groupBy(me -> keyPartitioner.getSegment(me.getKey()))
            .flatMapCompletable(groupedFlowable ->
                  groupedFlowable
                        .buffer(configuration.maxBatchSize())
                        .flatMapCompletable(batch -> {
                           CompletionStage<Void> stage = stores.get(groupedFlowable.getKey()).bulkUpdate(Flowable.fromIterable(batch));
                           return Completable.fromCompletionStage(stage);
                           // Make sure to set the parallelism level to how many groups will be created
                        }), false, stores.length())
            .toCompletionStage(null);
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      cache = ctx.getCache();
   }

   @Override
   public void start() {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      HashConfiguration hashConfiguration = cache.getCacheConfiguration().clustering().hash();
      keyPartitioner = componentRegistry.getComponent(KeyPartitioner.class);
      stores = new AtomicReferenceArray<>(hashConfiguration.numSegments());

      // Local (invalidation) and replicated we just instantiate all the maps immediately
      // Distributed needs them all only at beginning for preload of data - rehash event will remove others
      for (int i = 0; i < stores.length(); ++i) {
         startNewStoreForSegment(i);
      }

      // Distributed is the only mode that allows for dynamic addition/removal of maps as others own all segments
      // in some fashion - others will clear instead when segment ownership is lost
      shouldStopSegments = cache.getCacheConfiguration().clustering().cacheMode().isDistributed();
   }

   private void startNewStoreForSegment(int segment) {
      if (stores.get(segment) == null) {
         T storeConfiguration = configuration.newConfigurationFrom(segment, ctx);
         AdvancedLoadWriteStore<K, V> newStore = PersistenceUtil.createStoreInstance(storeConfiguration);
         newStore.init(new InitializationContextImpl(storeConfiguration, cache, keyPartitioner, ctx.getPersistenceMarshaller(), ctx.getTimeService(),
               ctx.getByteBufferFactory(), ctx.getMarshallableEntryFactory(), ctx.getNonBlockingExecutor(), ctx.getGlobalConfiguration(),
               ctx.getBlockingManager(), ctx.getNonBlockingManager()));
         newStore.start();
         stores.set(segment, newStore);
      }
   }

   private void stopStoreForSegment(int segment) {
      AdvancedLoadWriteStore<K, V> store = stores.getAndSet(segment, null);
      if (store != null) {
         store.stop();
      }
   }

   private void destroyStore(int segment) {
      AdvancedLoadWriteStore<K, V> store = stores.getAndSet(segment, null);
      if (store != null) {
         store.destroy();
      }
   }

   @Override
   public void stop() {
      for (int i = 0; i < stores.length(); ++i) {
         stopStoreForSegment(i);
      }
   }

   @Override
   public void destroy() {
      for (int i = 0; i < stores.length(); ++i) {
         destroyStore(i);
      }
   }

   @Override
   public void addSegments(IntSet segments) {
      segments.forEach((IntConsumer) this::startNewStoreForSegment);
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (shouldStopSegments) {
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            destroyStore(segmentIterator.nextInt());
         }
      } else {
         clear(segments);
      }
   }

   /**
    * Method that allows user to directly invoke some method(s) on the underlying store. The segment that each
    * store maps to is also provided as an argument to the consumer
    * @param consumer callback for every store that is currently installed
    */
   public void forEach(ObjIntConsumer<? super AdvancedLoadWriteStore> consumer) {
      for (int i = 0; i < stores.length(); ++i) {
         AdvancedLoadWriteStore store = stores.get(i);
         if (store != null) {
            consumer.accept(store, i);
         }
      }
   }
}
