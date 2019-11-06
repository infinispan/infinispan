package org.infinispan.persistence.dummy;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.AbstractSegmentedAdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;
import org.reactivestreams.Publisher;
import org.testng.AssertJUnit;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;

/**
 * A Dummy cache store which stores objects in memory. Instance of the store can be shared
 * amongst multiple caches by utilising the same `storeName` for each store instance.
 */
@ConfiguredBy(DummyInMemoryStoreConfiguration.class)
@Store(shared = true)
public class DummyInMemoryStore extends AbstractSegmentedAdvancedLoadWriteStore implements AdvancedCacheExpirationWriter {
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
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.keyPartitioner = ctx.getKeyPartitioner();
      this.cache = ctx.getCache();
      this.marshaller = ctx.getPersistenceMarshaller();
      this.storeName = makeStoreName(configuration, cache);
      this.initCount.incrementAndGet();
      this.timeService = ctx.getTimeService();
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
   protected ToIntFunction<Object> getKeyMapper() {
      return configuration.segmented() ? keyPartitioner : SingleSegmentKeyPartitioner.getInstance();
   }

   @Override
   public void write(int segment, MarshallableEntry entry) {
      assertRunning();
      record("write");
      if (configuration.slow()) {
         TestingUtil.sleepThread(SLOW_STORE_WAIT);
      }
      if (entry!= null) {
         Map<Object, byte[]> map = mapForSegment(segment);
         if (trace) log.tracef("Store %s in dummy map store@%s", entry, Util.hexIdHashCode(store));
         map.put(entry.getKey(), serialize(entry));
      }
   }

   @Override
   public void clear() {
      clear(IntSets.immutableRangeSet(store.length()));
   }

   @Override
   public boolean delete(int segment, Object key) {
      assertRunning();
      record("delete");
      Map<Object, byte[]> map = mapForSegment(segment);
      if (map.remove(key) != null) {
         if (trace) log.tracef("Removed %s from dummy store", key);
         return true;
      }

      if (trace) log.tracef("Key %s not present in store, so don't remove", key);
      return false;
   }

   @Override
   public void purge(Executor executor, ExpirationPurgeListener listener) {
      long currentTimeMillis = timeService.wallClockTime();
      Set expired = new HashSet();
      for (int i = 0; i < store.length(); ++i) {
         Map<Object, byte[]> map = store.get(i);
         if (map == null) {
            continue;
         }
         for (Iterator<Map.Entry<Object, byte[]>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<Object, byte[]> next = iter.next();
            MarshallableEntry se = deserialize(next.getKey(), next.getValue());
            if (isExpired(se, currentTimeMillis)) {
               if (listener != null) listener.marshalledEntryPurged(se);
               iter.remove();
               expired.add(next.getKey());
            }
         }
      }
   }

   @Override
   public MarshallableEntry get(int segment, Object key) {
      assertRunning();
      record("load");
      if (key == null) return null;
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry me = deserialize(key, map.get(key));
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
   public Flowable<MarshallableEntry> entryPublisher(Predicate filter, boolean fetchValue, boolean fetchMetadata) {
      return entryPublisher(IntSets.immutableRangeSet(segmentCount), filter, fetchValue, fetchMetadata);
   }

   @Override
   public Flowable<MarshallableEntry> entryPublisher(IntSet segments, Predicate filter, boolean fetchValue, boolean fetchMetadata) {
      assertRunning();
      record("publishEntries");
      log.tracef("Publishing entries in store %s with filter %s", storeName, filter);
      Flowable<Map.Entry<Object, byte[]>> flowable;
      if (configuration.segmented()) {
       flowable = new FlowableFromIntSetFunction<>(segments, segment -> {
            Map<Object, byte[]> map = store.get(segment);
            if (map == null) {
               return Flowable.<Map.Entry<Object, byte[]>>empty();
            }
            return Flowable.fromIterable(map.entrySet());
         }).flatMap(Functions.identity());
      } else {
         flowable = Flowable.fromIterable(store.get(0).entrySet())
            .filter(e -> {
               int segment = keyPartitioner.getSegment(e.getKey());
               return segments.contains(segment);
            });
      }

      return flowable.compose(f -> {
         // We compose so current time millis is retrieved for each subscriber
         final long currentTimeMillis = timeService.wallClockTime();
         if (filter != null) {
            f = f.filter(e -> filter.test(e.getKey()));
         }
         if (configuration.slow()) {
            f = f.doOnNext(ignore -> Thread.sleep(SLOW_STORE_WAIT));
         }
         return f.map(entry -> deserialize(entry.getKey(), entry.getValue()))
               .filter(me -> !isExpired(me, currentTimeMillis));
      });
   }

   @Override
   public void start() {
      if (store != null)
         return;

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
   }

   private ConcurrentMap<String, AtomicInteger> newStatsMap() {
      ConcurrentMap<String, AtomicInteger> m = new ConcurrentHashMap<>();
      for (Method method: AdvancedCacheLoader.class.getMethods()) {
         m.put(method.getName(), new AtomicInteger(0));
      }
      for (Method method: AdvancedCacheWriter.class.getMethods()) {
         m.put(method.getName(), new AtomicInteger(0));
      }
      return m;
   }

   @Override
   public void stop() {
      record("stop");
      running = false;
      available = false;

      store = null;
   }

   @Override
   public boolean isAvailable() {
      return available;
   }

   public void setAvailable(boolean available) {
      this.available = available;
   }

   public String getStoreName() {
      return storeName;
   }

   public static int getStoreDataSize(String storeName) {
      AtomicReferenceArray<Map<Object, byte[]>> store = stores.get(storeName);
      return store != null ? size(IntSets.immutableRangeSet(store.length()), store) : 0;
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
   public int size() {
      return size(IntSets.immutableRangeSet(store.length()));
   }

   @Override
   public int size(IntSet segments) {
      record("size");
      return size(segments, store);
   }

   private static int size(IntSet segments, AtomicReferenceArray<Map<Object, byte[]>> store) {
      int size = 0;
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Map<Object, byte[]> map = store.get(segment);
         if (map != null) {
            size += map.size();
         }
         if (size < 0) {
            return Integer.MAX_VALUE;
         }
      }
      return size;
   }

   @Override
   public Publisher publishKeys(IntSet segments, Predicate filter) {
      return entryPublisher(segments, filter, false, true).map(MarshallableEntry::getKey);
   }

   @Override
   public void clear(IntSet segments) {
      assertRunning();
      record("clear");
      if (trace) log.trace("Clear store");
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         Map<Object, byte[]> map = store.get(segment);
         if (map != null) {
            map.clear();
         }
      }
   }

   @Override
   public boolean contains(int segment, Object key) {
      assertRunning();
      record("load");
      if (key == null) return false;
      Map<Object, byte[]> map = mapForSegment(segment);
      MarshallableEntry me = deserialize(key, map.get(key));
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
   public void addSegments(IntSet segments) {
      if (configuration.segmented() && storeName == null) {
         segments.forEach((int segment) -> {
            if (store.get(segment) == null) {
               store.set(segment, new ConcurrentHashMap<>());
            }
         });
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (configuration.segmented() && storeName == null) {
         segments.forEach((int segment) -> store.getAndSet(segment, null));
      }
   }

   public DummyInMemoryStoreConfiguration getConfiguration() {
      return configuration;
   }
}
