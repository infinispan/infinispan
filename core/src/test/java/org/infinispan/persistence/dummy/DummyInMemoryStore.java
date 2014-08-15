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

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@ConfiguredBy(DummyInMemoryStoreConfiguration.class)
public class DummyInMemoryStore implements AdvancedLoadWriteStore {
   private static final Log log = LogFactory.getLog(DummyInMemoryStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();
   static final ConcurrentMap<String, Map<Object, byte[]>> stores = new ConcurrentHashMap<String, Map<Object, byte[]>>();
   static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> storeStats =
         new ConcurrentHashMap<String, ConcurrentMap<String, AtomicInteger>>();
   String storeName;
   Map<Object, byte[]> store;
   // When a store is 'shared', multiple nodes could be trying to update it concurrently.
   ConcurrentMap<String, AtomicInteger> stats;
   public AtomicInteger initCount = new AtomicInteger();
   private TimeService timeService;
   Cache cache;

   protected volatile StreamingMarshaller marshaller;

   private DummyInMemoryStoreConfiguration configuration;
   private InitializationContext ctx;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.cache = ctx.getCache();
      this.marshaller = ctx.getMarshaller();
      this.storeName = configuration.storeName();
      this.initCount.incrementAndGet();
      this.timeService = ctx.getTimeService();
   }

   public DummyInMemoryStore(String storeName) {
      this.storeName = storeName;
   }

   public DummyInMemoryStore() {
   }

   private void record(String method) {
      stats.get(method).incrementAndGet();
   }


   @Override
   public void write(MarshalledEntry entry) {
//      System.out.println("[" + Thread.currentThread().getName() + "] entry.getKey() = " + entry.getKey());
      record("write");
      if (configuration.slow()) {
         TestingUtil.sleepThread(100);
      }
      if (entry!= null) {
         if (debug) log.tracef("Store %s in dummy map store@%s", entry, Util.hexIdHashCode(store));
         configuration.failKey();
         store.put(entry.getKey(), serialize(entry));
      }
   }

   @Override
   public void clear() {
      record("clear");
      if (trace) log.trace("Clear store");
      store.clear();
   }

   @Override
   public boolean delete(Object key) {
      record("delete");
      if (store.remove(key) != null) {
         if (debug) log.tracef("Removed %s from dummy store", key);
         return true;
      }

      if (debug) log.tracef("Key %s not present in store, so don't remove", key);
      return false;
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      long currentTimeMillis = timeService.wallClockTime();
      Set expired = new HashSet();
      for (Iterator<Map.Entry<Object, byte[]>> i = store.entrySet().iterator(); i.hasNext();) {
         Map.Entry<Object, byte[]> next = i.next();
         MarshalledEntry se = deserialize(next.getKey(), next.getValue(), false, true);
         if (isExpired(se, currentTimeMillis)) {
            if (task != null) task.entryPurged(next.getKey());
            i.remove();
            expired.add(next.getKey());
         }
      }
   }

   @Override
   public MarshalledEntry load(Object key) {
      record("load");
      if (key == null) return null;
      MarshalledEntry me = deserialize(key, store.get(key), true, true);
      if (me == null) return null;
      long now = timeService.wallClockTime();
      if (isExpired(me, now)) {
         log.tracef("Key %s exists, but has expired.  Entry is %s", key, me);
         store.remove(key);
         return null;
      }
      return me;
   }

   private boolean isExpired(MarshalledEntry me, long now) {
      return me.getMetadata() != null && me.getMetadata().isExpired(now);
   }

   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      record("process");
      log.tracef("Processing entries in store %s with filter %s and callback %s", storeName, filter, task);
      final long currentTimeMillis = timeService.wallClockTime();
      TaskContext tx = new TaskContextImpl();
      for (Iterator<Map.Entry<Object, byte[]>> i = store.entrySet().iterator(); i.hasNext();) {
         Map.Entry<Object, byte[]> entry = i.next();
         if (tx.isStopped()) break;
         if (filter == null || filter.accept(entry.getKey())) {
            MarshalledEntry se = deserialize(entry.getKey(), entry.getValue(), fetchValue, fetchMetadata);
            if (isExpired(se, currentTimeMillis)) {
               log.tracef("Key %s exists, but has expired.  Entry is %s", entry.getKey(), se);
               i.remove();
            } else {
               try {
                  task.processEntry(se,tx);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
               }
            }
         }
      }
   }

   @Override
   public void start() {
      if (store != null)
         return;

      Equivalence<Object> keyEq = cache.getCacheConfiguration().dataContainer().keyEquivalence();
      Equivalence<byte[]> valueEq = ByteArrayEquivalence.INSTANCE;
      store = new EquivalentConcurrentHashMapV8<Object, byte[]>(keyEq, valueEq);
      stats = newStatsMap();

      if (storeName != null) {
         if (cache != null) storeName += "_" + cache.getName();

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
   }

   private ConcurrentMap<String, AtomicInteger> newStatsMap() {
      ConcurrentMap<String, AtomicInteger> m = new ConcurrentHashMap<String, AtomicInteger>();
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

      if (configuration.purgeOnStartup()) {
         if (storeName != null) {
            stores.remove(storeName);
         }
      }
   }

   public boolean isEmpty() {
      return store.isEmpty();
   }

   public Set<Object> keySet() {
      return store.keySet();
   }

   public Map<String, Integer> stats() {
      Map<String, Integer> copy = new HashMap<String, Integer>(stats.size());
      for (String k: stats.keySet()) copy.put(k, stats.get(k).get());
      return copy;
   }

   public void clearStats() {
      for (String k: stats.keySet()) stats.get(k).set(0);
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
      return store.size();
   }

   @Override
   public boolean contains(Object key) {
      return store.containsKey(key);
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
         if (!fetchValue && !fetchMetadata) {
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, null, (InternalMetadata) null);
         }
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

}
