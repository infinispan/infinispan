package org.infinispan.core.test.jupiter;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Simple in-memory {@link NonBlockingStore} for validating the store test harness.
 * <p>
 * Uses segmented storage with marshalling round-trips, similar to
 * {@code DummyInMemoryStore} in the core module but without shared stores,
 * statistics, or async/slow modes.
 *
 * @since 16.2
 */
public class SimpleInMemoryStore<K, V> implements NonBlockingStore<K, V> {

   @SuppressWarnings("unchecked")
   private ConcurrentMap<Object, byte[]>[] segments = new ConcurrentMap[0];
   private int segmentCount;
   private TimeService timeService;
   private PersistenceMarshaller marshaller;
   private MarshallableEntryFactory<K, V> entryFactory;
   private volatile boolean running;

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE);
   }

   @Override
   @SuppressWarnings("unchecked")
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.marshaller = ctx.getPersistenceMarshaller();
      this.entryFactory = ctx.getMarshallableEntryFactory();
      this.timeService = ctx.getTimeService();
      this.segmentCount = ctx.getCache().getCacheConfiguration()
            .clustering().hash().numSegments();

      if (this.segments == null || this.segments.length != segmentCount) {
         this.segments = new ConcurrentMap[segmentCount];
         for (int i = 0; i < segmentCount; i++) {
            segments[i] = new ConcurrentHashMap<>();
         }
      }
      running = true;
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> stop() {
      running = false;
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      segments[segment].put(entry.getKey(), serialize(entry));
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      byte[] bytes = segments[segment].get(key);
      MarshallableEntry<K, V> entry = deserialize(key, bytes);
      if (entry != null && entry.isExpired(timeService.wallClockTime())) {
         return CompletableFutures.completedNull();
      }
      return entry == null
            ? CompletableFutures.completedNull()
            : CompletableFuture.completedFuture(entry);
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return segments[segment].remove(key) != null
            ? CompletableFutures.completedTrue()
            : CompletableFutures.completedFalse();
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      byte[] bytes = segments[segment].get(key);
      if (bytes == null) return CompletableFutures.completedFalse();
      MarshallableEntry<K, V> entry = deserialize(key, bytes);
      if (entry != null && entry.isExpired(timeService.wallClockTime())) {
         return CompletableFutures.completedFalse();
      }
      return CompletableFutures.completedTrue();
   }

   @Override
   public CompletionStage<Void> clear() {
      for (ConcurrentMap<Object, byte[]> segment : segments) {
         if (segment != null) segment.clear();
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return CompletableFutures.completedTrue();
   }

   @Override
   @SuppressWarnings("unchecked")
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
                                                            boolean fetchValue) {
      return Flowable.defer(() -> {
         long now = timeService.wallClockTime();
         return Flowable.fromIterable(segments)
               .concatMap(segment -> {
                  ConcurrentMap<Object, byte[]> map = this.segments[segment];
                  if (map == null || map.isEmpty()) return Flowable.empty();
                  return Flowable.fromIterable(map.entrySet())
                        .map(e -> (MarshallableEntry<K, V>) deserialize(e.getKey(), e.getValue()))
                        .filter(e -> !e.isExpired(now))
                        .filter(e -> filter == null || filter.test(e.getKey()));
               });
      });
   }

   @Override
   @SuppressWarnings("unchecked")
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return Flowable.defer(() -> {
         long now = timeService.wallClockTime();
         return Flowable.range(0, segmentCount)
               .concatMap(segment -> {
                  ConcurrentMap<Object, byte[]> map = this.segments[segment];
                  if (map == null) return Flowable.empty();
                  return Flowable.fromIterable(map.entrySet())
                        .map(e -> (MarshallableEntry<K, V>) deserialize(e.getKey(), e.getValue()))
                        .filter(e -> e.isExpired(now))
                        .doOnNext(e -> map.remove(e.getKey()));
               });
      });
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      long now = timeService.wallClockTime();
      long count = 0;
      for (int segment : segments) {
         ConcurrentMap<Object, byte[]> map = this.segments[segment];
         if (map != null) {
            for (var entry : map.entrySet()) {
               MarshallableEntry<K, V> me = deserialize(entry.getKey(), entry.getValue());
               if (!me.isExpired(now)) count++;
            }
         }
      }
      return CompletableFuture.completedFuture(count);
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      long count = 0;
      for (int segment : segments) {
         ConcurrentMap<Object, byte[]> map = this.segments[segment];
         if (map != null) count += map.size();
      }
      return CompletableFuture.completedFuture(count);
   }

   @Override
   @SuppressWarnings("unchecked")
   public CompletionStage<Void> addSegments(IntSet segments) {
      for (int segment : segments) {
         if (this.segments[segment] == null) {
            this.segments[segment] = new ConcurrentHashMap<>();
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      for (int segment : segments) {
         this.segments[segment] = null;
      }
      return CompletableFutures.completedNull();
   }

   private byte[] serialize(MarshallableEntry<? extends K, ? extends V> entry) {
      try {
         return marshaller.objectToByteBuffer(entry.getMarshalledValue());
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }

   private MarshallableEntry<K, V> deserialize(Object key, byte[] bytes) {
      if (bytes == null) return null;
      try {
         MarshalledValue value = (MarshalledValue) marshaller.objectFromByteBuffer(bytes);
         return entryFactory.create(key, value);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }
}
