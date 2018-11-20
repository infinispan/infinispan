package org.infinispan.persistence.dummy;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.KeyValuePair;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;

/**
 * A Dummy cache store which stores objects in memory. Instance of the store can be shared
 * amongst multiple caches by utilising the same `storeName` for each store instance.
 */
@ConfiguredBy(DummyInMemoryStoreConfiguration.class)
@Store(shared = true)
public class DummyInMemoryStore implements AdvancedLoadWriteStore, AdvancedCacheExpirationWriter {
   public static final int SLOW_STORE_WAIT = 100;

   private static final Log log = LogFactory.getLog(DummyInMemoryStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final ConcurrentMap<String, Map<Object, byte[]>> stores = new ConcurrentHashMap<>();
   private static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> storeStats = new ConcurrentHashMap<>();

   private String storeName;
   private Map<Object, byte[]> store;
   // When a store is 'shared', multiple nodes could be trying to update it concurrently.
   private ConcurrentMap<String, AtomicInteger> stats;
   private AtomicInteger initCount = new AtomicInteger();
   private TimeService timeService;
   private Cache cache;
   private StreamingMarshaller marshaller;
   private DummyInMemoryStoreConfiguration configuration;
   private InitializationContext ctx;
   private volatile boolean running;
   private volatile boolean available;
   private AtomicInteger startAttempts = new AtomicInteger();

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.cache = ctx.getCache();
      this.marshaller = ctx.getMarshaller();
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

   @Override
   public void write(MarshalledEntry entry) {
      assertRunning();
      record("write");
      if (configuration.slow()) {
         TestingUtil.sleepThread(SLOW_STORE_WAIT);
      }
      if (entry!= null) {
         if (trace) log.tracef("Store %s in dummy map store@%s", entry, Util.hexIdHashCode(store));
         store.put(entry.getKey(), serialize(entry));
      }
   }

   @Override
   public void clear() {
      assertRunning();
      record("clear");
      if (trace) log.trace("Clear store");
      store.clear();
   }

   @Override
   public boolean delete(Object key) {
      assertRunning();
      record("delete");
      if (store.remove(key) != null) {
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
      for (Iterator<Map.Entry<Object, byte[]>> i = store.entrySet().iterator(); i.hasNext();) {
         Map.Entry<Object, byte[]> next = i.next();
         MarshalledEntry se = deserialize(next.getKey(), next.getValue(), true, true);
         if (isExpired(se, currentTimeMillis)) {
            if (listener != null) listener.marshalledEntryPurged(se);
            i.remove();
            expired.add(next.getKey());
         }
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      assertRunning();
      record("load");
      if (key == null) return null;
      MarshalledEntry me = deserialize(key, store.get(key), true, true);
      if (me == null) return null;
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return null;
      }
      return me;
   }

   private boolean isExpired(MarshalledEntry me, long now) {
      return me.getMetadata() != null && me.getMetadata().isExpired(now);
   }

   @Override
   public Flowable<MarshalledEntry> publishEntries(Predicate filter, boolean fetchValue, boolean fetchMetadata) {
      assertRunning();
      record("publishEntries");
      log.tracef("Publishing entries in store %s with filter %s", storeName, filter);
      Flowable<Map.Entry<Object, byte[]>> flowable = Flowable.fromIterable(store.entrySet());
      return flowable.compose(f -> {
         // We compose so current time millis is retrieved for each subscriber
         final long currentTimeMillis = timeService.wallClockTime();
         if (filter != null) {
            f = f.filter(e -> filter.test(e.getKey()));
         }
         return f.map(entry -> deserialize(entry.getKey(), entry.getValue(), fetchValue, fetchMetadata))
               .filter(me -> !isExpired(me, currentTimeMillis));
      });
   }

   @Override
   public void start() {
      if (store != null)
         return;

      if (configuration.startFailures() > startAttempts.incrementAndGet())
         throw new PersistenceException();

      store = new ConcurrentHashMap<>();
      stats = newStatsMap();

      if (storeName != null) {
         Map<Object, byte[]> existing = stores.putIfAbsent(storeName, store);
         if (existing != null) {
            store = existing;
            log.debugf("Reusing in-memory cache store %s", storeName);
         } else {
            log.debugf("Creating new in-memory cache store %s", storeName);
         }

         ConcurrentMap<String, AtomicInteger> existingStats = storeStats.putIfAbsent(storeName, stats);
         if (existing != null) {
            stats = existingStats;
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

      if (configuration.purgeOnStartup()) {
         if (storeName != null) {
            removeStoreData(storeName);
         }
      }
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
      Map<Object, byte[]> storeMap = stores.get(storeName);
      return storeMap != null ? storeMap.size() : 0;
   }

   public static void removeStoreData(String storeName) {
      stores.remove(storeName);
   }

   public boolean isEmpty() {
      return store.isEmpty();
   }

   public Set<Object> keySet() {
      return store.keySet();
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
      long killTime = timeService.wallClockTime() + timeout;
      while (timeService.wallClockTime() < killTime) {
         MarshalledEntry entry = deserialize(key, store.get(key), true, false);
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
         notRemoved = InfinispanCollections.difference(store.keySet(), expectedState);
         // Find out which entries might not have been stored
         notStored = InfinispanCollections.difference(expectedState, store.keySet());
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
      record("size");
      return store.size();
   }

   @Override
   public boolean contains(Object key) {
      assertRunning();
      record("load");
      if (key == null) return false;
      MarshalledEntry me = deserialize(key, store.get(key), false, true);
      if (me == null) return false;
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         return false;
      }
      return true;
   }

   private byte[] serialize(MarshalledEntry o) {
      try {
         return marshaller.objectToByteBuffer(new KeyValuePair(o.getValue(), o.getMetadata()));
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   private MarshalledEntry deserialize(Object key, byte[] b, boolean fetchValue, boolean fetchMetadata) {
      try {
         if (b == null)
            return null;
         // We have to fetch metadata to tell if a key or entry is expired. Note this can be changed
         // after API changes
         fetchMetadata = true;
         KeyValuePair<Object, InternalMetadata> keyValuePair =
               (KeyValuePair<Object, InternalMetadata>) marshaller.objectFromByteBuffer(b);
         return ctx.getMarshalledEntryFactory().newMarshalledEntry(key,
               fetchValue ? keyValuePair.getKey() : null,
               fetchMetadata ? keyValuePair.getValue() : null);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   private void assertRunning() {
      if (!running)
         throw new IllegalLifecycleStateException();

      if (!available)
         throw new PersistenceException();
   }
}
