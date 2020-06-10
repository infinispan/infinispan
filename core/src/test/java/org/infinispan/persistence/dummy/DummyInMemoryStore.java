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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.testng.AssertJUnit;

import io.reactivex.rxjava3.core.Flowable;

/**
 * A Dummy cache store which stores objects in memory. Instance of the store can be shared
 * amongst multiple caches by utilising the same `storeName` for each store instance.
 */
@ConfiguredBy(DummyInMemoryStoreConfiguration.class)
@Store(shared = true)
public class DummyInMemoryStore implements WaitNonBlockingStore {
   public static final int SLOW_STORE_WAIT = 100;

   private static final Log log = LogFactory.getLog(DummyInMemoryStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final ConcurrentMap<String, AtomicReferenceArray<Map<Object, byte[]>>> stores = new ConcurrentHashMap<>();
   private static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> storeStats = new ConcurrentHashMap<>();

   private String storeName;
   private AtomicReferenceArray<Map<Object, byte[]>> store;
   // When a store is 'shared', multiple nodes could be trying to update it concurrently.
   private ConcurrentMap<String, AtomicInteger> stats;
   private int segmentCount;
   private AtomicInteger initCount = new AtomicInteger();
   private TimeService timeService;
   private Cache cache;
   private PersistenceMarshaller marshaller;
   private DummyInMemoryStoreConfiguration configuration;
   private KeyPartitioner keyPartitioner;
   private InitializationContext ctx;
   private volatile boolean running;
   private volatile boolean available;
   private AtomicInteger startAttempts = new AtomicInteger();

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.keyPartitioner = ctx.getKeyPartitioner();
      this.cache = ctx.getCache();
      this.marshaller = ctx.getPersistenceMarshaller();
      this.storeName = makeStoreName(configuration, cache);
      this.initCount.incrementAndGet();
      this.timeService = ctx.getTimeService();

      if (store != null)
         return CompletableFutures.completedNull();

      if (configuration.startFailures() > startAttempts.incrementAndGet())
         throw new PersistenceException();

      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      segmentCount = clusteringConfiguration.hash().numSegments();

      int segmentsToInitialize;
      if (configuration.segmented()) {
         segmentsToInitialize = segmentCount;
      } else {
         segmentsToInitialize = 1;
      }

      store = new AtomicReferenceArray<>(segmentsToInitialize);
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
            TestResourceTracker.addResource(new TestResourceTracker.Cleaner<String>(storeName) {
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
         for (int i = 0; i < segmentsToInitialize; ++i) {
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

   private String makeStoreName(DummyInMemoryStoreConfiguration configuration, Cache cache) {
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
      if (entry!= null) {
         Map<Object, byte[]> map = mapForSegment(segment);
         if (trace) log.tracef("Store %s for segment %s in dummy map store@%s", entry, segment, Util.hexIdHashCode(store));
         map.put(entry.getKey(), serialize(entry));
      }

      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> clear() {
      assertRunning();
      record("clear");
      if (trace) log.trace("Clear store");
      for (int i = 0; i < store.length(); ++i) {
         Map<Object, byte[]> map = store.get(i);
         if (map != null) {
            map.clear();
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      assertRunning();
      record("delete");
      Map<Object, byte[]> map = mapForSegment(segment);
      if (map.remove(key) != null) {
         if (trace) log.tracef("Removed %s from dummy store for segment %s", key, segment);
         return CompletableFutures.completedTrue();
      }

      if (trace) log.tracef("Key %s not present in store, so don't remove", key);
      return CompletableFutures.completedFalse();
   }

   @Override
   public Publisher<MarshallableEntry> purgeExpired() {
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
   public CompletionStage<MarshallableEntry> load(int segment, Object key) {
      assertRunning();
      record("load");
      if (key == null) return null;
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry me = deserialize(key, map.get(key));
      if (me == null) return CompletableFutures.completedNull();
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return CompletableFutures.completedNull();
      }
      return CompletableFuture.completedFuture(me);
   }

   private boolean isExpired(MarshallableEntry me, long now) {
      return me.isExpired(now);
   }

   @Override
   public Flowable<MarshallableEntry> publishEntries(IntSet segments, Predicate filter, boolean fetchValue) {
      assertRunning();
      record("publishEntries");
      log.tracef("Publishing entries in store %s with filter %s", storeName, filter);
      Flowable<Map.Entry<Object, byte[]>> flowable;
      if (configuration.segmented()) {
         flowable = Flowable.fromStream(segments.intStream().mapToObj(segment -> {
            Map<Object, byte[]> map = store.get(segment);
            if (map == null) {
               return Flowable.<Map.Entry<Object, byte[]>>empty();
            }
            return Flowable.fromIterable(map.entrySet());
         })).flatMap(RxJavaInterop.identityFunction());
      } else {
         flowable = Flowable.fromIterable(store.get(0).entrySet())
               .filter(e -> {
                  int segment = keyPartitioner.getSegment(e.getKey());
                  return segments.contains(segment);
               });
      }

      if (filter != null) {
         flowable = flowable.filter(e -> filter.test(e.getKey()));
      }
      if (configuration.slow()) {
         flowable = flowable.doOnNext(ignore -> Thread.sleep(SLOW_STORE_WAIT));
      }

      Flowable<MarshallableEntry> meFlowable = flowable.map(entry -> deserialize(entry.getKey(), entry.getValue()));

      // Defer the check for time until subscriber so it can be subscribed to at different times
      return Flowable.defer(() -> {
         final long currentTimeMillis = timeService.wallClockTime();
         return meFlowable.filter(me -> !isExpired(me, currentTimeMillis));
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
      if (running) {
         record("stop");
         running = false;
         available = false;

         store = null;
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return CompletableFutures.booleanStage(available);
   }

   public void setAvailable(boolean available) {
      this.available = available;
   }

   public String getStoreName() {
      return storeName;
   }

   public static long getStoreDataSize(String storeName) {
      AtomicReferenceArray<Map<Object, byte[]>> store = stores.get(storeName);
      return store != null ? CompletionStages.join(size(IntSets.immutableRangeSet(store.length()), store)) : 0;
   }

   public static void removeStoreData(String storeName) {
      stores.remove(storeName);
   }

   public static void removeStatData(String storeName) {
      storeStats.remove(storeName);
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

   public void blockUntilCacheStoreContains(Object key, Object expectedValue, long timeout) {
      Map<Object, byte[]> map = mapForSegment(keyPartitioner.getSegment(key));
      AssertJUnit.assertNotNull("Map for key " + key + " was not present", map);
      long killTime = timeService.wallClockTime() + timeout;
      while (timeService.wallClockTime() < killTime) {
         MarshallableEntry entry = deserialize(key, map.get(key));
         if (entry != null && entry.getValue().equals(expectedValue)) return;
         TestingUtil.sleepThread(50);
      }
      throw new RuntimeException(String.format(
            "Timed out waiting (%d ms) for cache store to contain key=%s with value=%s",
            timeout, key, expectedValue));
   }

   public void blockUntilCacheStoreContains(Set<Object> expectedState, long timeout) {
      long killTime = timeService.wallClockTime() + timeout;
      // Set<? extends Map.Entry<?, InternalCacheEntry>> expectedEntries = expectedState.entrySet();
      Set<Object> notStored = null;
      Set<Object> notRemoved = null;
      while (timeService.wallClockTime() < killTime) {
         // Find out which entries might not have been removed from the store
         notRemoved = InfinispanCollections.difference(keySet(), expectedState);
         // Find out which entries might not have been stored
         notStored = InfinispanCollections.difference(expectedState, keySet());
         if (notStored.isEmpty() && notRemoved.isEmpty())
            break;

         TestingUtil.sleepThread(100);
      }

      if ((notStored != null && !notStored.isEmpty()) || (notRemoved != null && !notRemoved.isEmpty())) {
         if (log.isTraceEnabled()) {
            log.tracef("Entries still not stored: %s", notStored);
            log.tracef("Entries still not removed: %s", notRemoved);
         }
         throw new RuntimeException(String.format(
               "Timed out waiting (%d ms) for cache store to be flushed. entries-not-stored=[%s], entries-not-removed=[%s]",
               timeout, notStored, notRemoved));
      }
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      record("size");
      return size(segments, store);
   }

   /**
    * Helper method only invoked by tests
    */
   public long size() {
      return CompletionStages.join(size(IntSets.immutableRangeSet(store.length())));
   }

   private static CompletionStage<Long> size(IntSet segments, AtomicReferenceArray<Map<Object, byte[]>> store) {
      long size = 0;
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Map<Object, byte[]> map = store.get(segment);
         if (map != null) {
            size += map.size();
         }
      }
      return CompletableFuture.completedFuture(size);
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      assertRunning();
      record("containsKey");
      if (key == null) return CompletableFutures.completedFalse();
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry me = deserialize(key, map.get(key));
      if (me == null) return CompletableFutures.completedFalse();
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return CompletableFutures.completedFalse();
      }
      return CompletableFutures.completedTrue();
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

   private MarshallableEntry deserialize(Object key, byte[] b) {
      try {
         if (b == null)
            return null;
         // We have to fetch metadata to tell if a key or entry is expired. Note this can be changed
         // after API changes
         MarshalledValue value = (MarshalledValue) marshaller.objectFromByteBuffer(b);
         return ctx.getMarshallableEntryFactory().create(key, value);
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
      record("addSegments");
      if (configuration.segmented() && storeName == null) {
         if (trace) log.tracef("Adding segments %s", segments);
         segments.forEach((int segment) -> {
            if (store.get(segment) == null) {
               store.set(segment, new ConcurrentHashMap<>());
            }
         });
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      assertRunning();
      record("removeSegments");
      if (configuration.segmented() && storeName == null) {
         if (trace) log.tracef("Removing segments %s", segments);
         segments.forEach((int segment) -> store.getAndSet(segment, null));
      }
      return CompletableFutures.completedNull();
   }

   public DummyInMemoryStoreConfiguration getConfiguration() {
      return configuration;
   }
}
