package org.infinispan.persistence.dummy;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * A Dummy cache store which stores objects in memory. Instance of the store can be shared
 * amongst multiple caches by utilising the same `storeName` for each store instance.
 */
@ConfiguredBy(DummyInMemoryStoreConfiguration.class)
@Store(shared = true)
public class DummyInMemoryStore<K, V> implements WaitNonBlockingStore<K, V> {
   public static final int SLOW_STORE_WAIT = 100;

   private static final Log log = LogFactory.getLog(DummyInMemoryStore.class);
   private static final ConcurrentMap<String, AtomicReferenceArray<Map<Object, byte[]>>> stores = new ConcurrentHashMap<>();
   private static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> storeStats = new ConcurrentHashMap<>();

   private String storeName;
   private AtomicReferenceArray<Map<Object, byte[]>> store;
   // When a store is 'shared', multiple nodes could be trying to update it concurrently.
   private ConcurrentMap<String, AtomicInteger> stats;
   private int segmentCount;
   private final AtomicInteger initCount = new AtomicInteger();
   private TimeService timeService;
   private PersistenceMarshaller marshaller;
   private DummyInMemoryStoreConfiguration configuration;
   private KeyPartitioner keyPartitioner;
   private Executor nonBlockingExecutor;
   private InitializationContext ctx;
   private volatile boolean running;
   private volatile boolean available;
   private volatile boolean exceptionOnAvailbilityCheck;
   private final AtomicInteger startAttempts = new AtomicInteger();

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.keyPartitioner = ctx.getKeyPartitioner();
      Cache<?, ?> cache = ctx.getCache();
      this.marshaller = ctx.getPersistenceMarshaller();
      this.storeName = makeStoreName(configuration, cache);
      this.initCount.incrementAndGet();
      this.timeService = ctx.getTimeService();
      this.nonBlockingExecutor = ctx.getNonBlockingExecutor();

      if (store != null)
         return CompletableFutures.completedNull();

      if (configuration.startFailures() > startAttempts.incrementAndGet())
         throw new PersistenceException();

      int segmentCount;
      if (configuration.segmented()) {
         ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
         segmentCount = clusteringConfiguration.hash().numSegments();
      } else {
         segmentCount = 1;
      }

      store = new AtomicReferenceArray<>(segmentCount);
      stats = newStatsMap();

      boolean shouldStartSegments = true;
      if (storeName != null) {
         AtomicReferenceArray<Map<Object, byte[]>> existing = stores.putIfAbsent(storeName, store);
         if (existing != null) {
            store = existing;
            log.debugf("Reusing in-memory cache store %s", storeName);
            shouldStartSegments = false;
         } else {
            // Clean up the array for this test
            TestResourceTracker.addResource(new TestResourceTracker.Cleaner<>(storeName) {
               @Override
               public void close() {
                  removeStoreData(ref);
                  storeStats.remove(ref);
               }
            });
            log.debugf("Creating new in-memory cache store %s", storeName);
         }

         ConcurrentMap<String, AtomicInteger> existingStats = storeStats.putIfAbsent(storeName, stats);
         if (existingStats != null) {
            stats = existingStats;
         }
      }

      if (shouldStartSegments) {
         for (int i = 0; i < segmentCount; ++i) {
            store.set(i, new ConcurrentHashMap<>());
         }
      }

      // record at the end!
      record("start");
      running = true;
      available = true;

      return CompletableFutures.completedNull();
   }

   @Override
   public KeyPartitioner getKeyPartitioner() {
      return keyPartitioner;
   }

   private String makeStoreName(DummyInMemoryStoreConfiguration configuration, Cache<?, ?> cache) {
      String configName = configuration.storeName();
      if (configName == null)
         return null;

      return cache != null ? configName + "_" + cache.getName() : configName;
   }

   public DummyInMemoryStore(String storeName) {
      this.storeName = storeName;
   }

   public DummyInMemoryStore() {
   }

   public boolean isRunning() {
      return running;
   }

   public int getInitCount() {
      return initCount.get();
   }

   private void record(String method) {
      stats.get(method).incrementAndGet();
   }

   private Map<Object, byte[]> mapForSegment(int segment) {
      if (!configuration.segmented()) {
         return store.get(0);
      }
      Map<Object, byte[]> map = store.get(segment);
      return map == null ? Collections.emptyMap() : map;
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SHAREABLE,
            Characteristic.SEGMENTABLE);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
      assertRunning();
      record("write");
      if (configuration.slow()) {
         TestingUtil.sleepThread(SLOW_STORE_WAIT);
      }

      if (configuration.asyncOperation()) {
         return CompletableFuture.runAsync(() -> actualWrite(segment, entry), nonBlockingExecutor);
      }
      actualWrite(segment, entry);
      return CompletableFutures.completedNull();
   }

   private void actualWrite(int segment, MarshallableEntry entry) {
      if (log.isTraceEnabled()) log.tracef("Store %s for segment %s in dummy map store@%s", entry, segment, Util.hexIdHashCode(store));
      Map<Object, byte[]> map = mapForSegment(segment);
      map.put(entry.getKey(), serialize(entry));
   }

   @Override
   public CompletionStage<Void> clear() {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.runAsync(this::actualClear, nonBlockingExecutor);
      }
      actualClear();
      return CompletableFutures.completedNull();
   }

   private void actualClear() {
      record("clear");
      if (log.isTraceEnabled()) log.trace("Clear store");
      for (int i = 0; i < store.length(); ++i) {
         Map<Object, byte[]> map = store.get(i);
         if (map != null) {
            map.clear();
         }
      }
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(() -> actualDelete(segment, key), nonBlockingExecutor);
      }

      return actualDelete(segment, key) ? CompletableFutures.completedTrue() : CompletableFutures.completedFalse();
   }

   private boolean actualDelete(int segment, Object key) {
      record("delete");
      Map<Object, byte[]> map = mapForSegment(segment);
      if (map.remove(key) != null) {
         if (log.isTraceEnabled()) log.tracef("Removed %s from dummy store for segment %s", key, segment);
         return true;
      }

      if (log.isTraceEnabled()) log.tracef("Key %s not present in store, so don't remove", key);
      return false;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      assertRunning();
      record("purgeExpired");
      return Flowable.defer(() -> {
         long currentTimeMillis = timeService.wallClockTime();
         return Flowable.range(0, store.length())
               .concatMap(offset -> {
                  Map<Object, byte[]> map = store.get(offset);
                  if (map == null) {
                     return Flowable.empty();
                  }
                  return Flowable.fromIterable(map.entrySet())
                        .map(entry -> deserialize(entry.getKey(), entry.getValue()))
                        .filter(me -> isExpired(me, currentTimeMillis))
                        .doOnNext(me -> map.remove(me.getKey()));
               });

      });
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(() -> actualLoad(segment, key), nonBlockingExecutor);
      }
      MarshallableEntry entry = actualLoad(segment, key);
      if (entry == null) {
         return CompletableFutures.completedNull();
      }
      return CompletableFuture.completedFuture(entry);
   }

   private MarshallableEntry actualLoad(int segment, Object key) {
      record("load");
      if (key == null) return null;
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry<K, V> me = deserialize(key, map.get(key));
      if (me == null) return null;
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return null;
      }
      return me;
   }

   private boolean isExpired(MarshallableEntry me, long now) {
      return me.isExpired(now);
   }

   @Override
   public Flowable<MarshallableEntry> publishEntries(IntSet segments, Predicate filter, boolean fetchValue) {
      assertRunning();
      record("publishEntries");
      log.tracef("Publishing entries in store %s segments %s with filter %s", storeName, segments, filter);
      Flowable<Map.Entry<Object, byte[]>> flowable;
      if (configuration.segmented()) {
         flowable = Flowable.fromIterable(segments)
                 .concatMap(segment -> {
                    Map<Object, byte[]> map = store.get(segment);
                    if (map == null || map.isEmpty()) {
                       return Flowable.<Map.Entry<Object, byte[]>>empty();
                    }
                    return Flowable.fromIterable(map.entrySet());
                 });
      } else {
         flowable = Flowable.fromIterable(store.get(0).entrySet());
      }

      Flowable<MarshallableEntry> meFlowable = flowable.map(entry -> deserialize(entry.getKey(), entry.getValue()));

      if (filter != null) {
         meFlowable = meFlowable.filter(e -> filter.test(e.getKey()));
      }
      if (configuration.slow()) {
         meFlowable = meFlowable.doOnNext(ignore -> Thread.sleep(SLOW_STORE_WAIT));
      }

      Flowable<MarshallableEntry> meFlowableFinal = meFlowable;

      // Defer the check for time, so it can be subscribed to at different times
      return Flowable.defer(() -> {
         final long currentTimeMillis = timeService.wallClockTime();
         return meFlowableFinal.filter(me -> !isExpired(me, currentTimeMillis));
      });
   }

   private ConcurrentMap<String, AtomicInteger> newStatsMap() {
      ConcurrentMap<String, AtomicInteger> m = new ConcurrentHashMap<>();
      for (Method method: NonBlockingStore.class.getMethods()) {
         m.put(method.getName(), new AtomicInteger(0));
      }
      return m;
   }

   @Override
   public CompletionStage<Void> stop() {
      if (configuration.asyncOperation()) {
         return CompletableFuture.runAsync(this::actualStop, nonBlockingExecutor);
      }
      actualStop();
      return CompletableFutures.completedNull();
   }

   private void actualStop() {
      if (running) {
         record("stop");
         running = false;
         available = false;

         store = null;
      }
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(this::actualIsAvailable, nonBlockingExecutor);
      }
      return CompletableFutures.booleanStage(actualIsAvailable());
   }

   private boolean actualIsAvailable() {
      if (exceptionOnAvailbilityCheck) {
         throw new RuntimeException();
      }
      return available;
   }

   public void setExceptionOnAvailbilityCheck(boolean throwException) {
      this.exceptionOnAvailbilityCheck = throwException;
   }

   public void setAvailable(boolean available) {
      log.debugf("Store availability change: %s -> %s", this.available, available);
      this.available = available;
   }

   public String getStoreName() {
      return storeName;
   }

   /**
    * @return a count of entries in all the segments, including expired entries
    */
   public long getStoreDataSize() {
      return sizeIncludingExpired(IntSets.immutableRangeSet(store.length()), store);
   }

   public static long getStoreDataSize(String storeName) {
      AtomicReferenceArray<Map<Object, byte[]>> store = stores.get(storeName);
      return store != null ? sizeIncludingExpired(IntSets.immutableRangeSet(store.length()), store) : 0;
   }

   public static void removeStoreData(String storeName) {
      stores.remove(storeName);
   }

   public static AtomicReferenceArray<Map<Object, byte[]>> getStoreDataForName(String storeName) {
      return stores.get(storeName);
   }

   public byte[] valueToStoredBytes(Object object) throws IOException, InterruptedException {
      ByteBuffer actualBytes = marshaller.objectToBuffer(object);
      MarshallableEntry<?, ?> me = ctx.getMarshallableEntryFactory().create(null, actualBytes);
      return marshaller.objectToByteBuffer(me.getMarshalledValue());
   }

   public boolean isEmpty() {
      for (int i = 0; i < store.length(); ++i) {
         Map<Object, byte[]> map = store.get(i);
         if (map != null && !map.isEmpty()) {
            return false;
         }
      }
      return true;
   }

   public Set<Object> keySet() {
      Set<Object> set = new HashSet<>();
      for (int i = 0; i < store.length(); ++i) {
         Map<Object, byte[]> map = store.get(i);
         if (map != null) {
            set.addAll(map.keySet());
         }
      }
      return set;
   }

   public Map<String, Integer> stats() {
      Map<String, Integer> copy = new HashMap<>(stats.size());
      for (String k: stats.keySet()) copy.put(k, stats.get(k).get());
      return copy;
   }

   public void clearStats() {
      for (AtomicInteger atomicInteger: stats.values()) atomicInteger.set(0);
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(() -> actualSize(segments), nonBlockingExecutor);
      }

      return CompletableFuture.completedFuture(actualSize(segments));
   }

   private long actualSize(IntSet segments) {
      record("size");

      AtomicLong size = new AtomicLong();
      long now = timeService.wallClockTime();
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Map<Object, byte[]> map = store.get(segment);
         if (map != null) {
            map.forEach((key, bytes) -> {
               MarshallableEntry<?, ?> me = deserialize(key, bytes);
               if (!me.isExpired(now)) {
                  size.incrementAndGet();
               }
            });
         }
      }
      return size.get();
   }

   /**
    * Helper method only invoked by tests
    */
   public long size() {
      return actualSize(IntSets.immutableRangeSet(store.length()));
   }

   private static long sizeIncludingExpired(IntSet segments, AtomicReferenceArray<Map<Object, byte[]>> store) {
      long size = 0;
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Map<Object, byte[]> map = store.get(segment);
         if (map != null) {
            size += map.size();
         }
      }
      return size;
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(() -> {
            record("size");
            return sizeIncludingExpired(segments, store);
         }, nonBlockingExecutor);
      }
      record("size");
      return CompletableFuture.completedFuture(sizeIncludingExpired(segments, store));
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.supplyAsync(() -> actualContainsKey(segment, key), nonBlockingExecutor);
      }

      return actualContainsKey(segment, key) ? CompletableFutures.completedTrue() : CompletableFutures.completedFalse();
   }

   private boolean actualContainsKey(int segment, Object key) {
      record("containsKey");
      if (key == null) return false;
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry<K, V> me = deserialize(key, map.get(key));
      if (me == null) return false;
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return false;
      }
      return true;
   }

   private byte[] serialize(MarshallableEntry entry) {
      try {
         return marshaller.objectToByteBuffer(entry.getMarshalledValue());
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   private MarshallableEntry<K, V> deserialize(Object key, byte[] b) {
      try {
         if (b == null)
            return null;
         // We have to fetch metadata to tell if a key or entry is expired. Note this can be changed
         // after API changes
         MarshalledValue value = (MarshalledValue) marshaller.objectFromByteBuffer(b);
         return (MarshallableEntry<K, V>) ctx.getMarshallableEntryFactory().create(key, value);
      } catch (ClassNotFoundException | IOException e) {
         throw new CacheException(e);
      }
   }

   private void assertRunning() {
      if (!running)
         throw new IllegalLifecycleStateException();

      if (!available)
         throw new PersistenceException();
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.runAsync(() -> actualAddSegments(segments), nonBlockingExecutor);
      }

      actualAddSegments(segments);
      return CompletableFutures.completedNull();
   }

   private void actualAddSegments(IntSet segments) {
      record("addSegments");
      if (configuration.segmented() && storeName == null) {
         if (log.isTraceEnabled()) log.tracef("Adding segments %s", segments);
         segments.forEach((int segment) -> {
            if (store.get(segment) == null) {
               store.set(segment, new ConcurrentHashMap<>());
            }
         });
      }
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      assertRunning();
      if (configuration.asyncOperation()) {
         return CompletableFuture.runAsync(() -> actualRemoveSegments(segments), nonBlockingExecutor);
      }

      actualRemoveSegments(segments);
      return CompletableFutures.completedNull();
   }

   private void actualRemoveSegments(IntSet segments) {
      record("removeSegments");
      if (configuration.segmented() && storeName == null) {
         if (log.isTraceEnabled()) log.tracef("Removing segments %s", segments);
         segments.forEach((int segment) -> store.getAndSet(segment, null));
      }
   }

   public DummyInMemoryStoreConfiguration getConfiguration() {
      return configuration;
   }
}
