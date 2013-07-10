package org.infinispan.loaders.dummy;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyInMemoryCacheStore extends AbstractCacheStore {
   private static final Log log = LogFactory.getLog(DummyInMemoryCacheStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();
   static final ConcurrentMap<String, Map<Object, byte[]>> stores = new ConcurrentHashMap<String, Map<Object, byte[]>>();
   static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> storeStats =
         new ConcurrentHashMap<String, ConcurrentMap<String, AtomicInteger>>();
   String storeName;
   Map<Object, byte[]> store;
   // When a store is 'shared', multiple nodes could be trying to update it concurrently.
   ConcurrentMap<String, AtomicInteger> stats;

   private DummyInMemoryCacheStoreConfiguration configuration;

   public DummyInMemoryCacheStore(String storeName) {
      this.storeName = storeName;
   }

   public DummyInMemoryCacheStore() {
   }

   private void record(String method) {
      stats.get(method).incrementAndGet();
   }

   @Override
   public void store(InternalCacheEntry ed) {
      record("store");
      if (configuration.slow()) {
         TestingUtil.sleepThread(100);
      }
      if (ed != null) {
         if (debug) log.debugf("Store %s in dummy map store@%s", ed, Util.hexIdHashCode(store));
         configuration.failKey();
         store.put(ed.getKey(), serializeEntry(ed));
      }
   }

   private byte[] serializeEntry(InternalCacheEntry ed) {
      try {
         return marshaller.objectToByteBuffer(ed);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   private InternalCacheEntry deserializeEntry(byte[] b) {
      try {
         if (b == null)
            return null;

         return (InternalCacheEntry) marshaller.objectFromByteBuffer(b);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public void fromStream(ObjectInput ois) throws CacheLoaderException {
      record("fromStream");
      try {
         int numEntries = (Integer) marshaller.objectFromObjectStream(ois);
         for (int i = 0; i < numEntries; i++) {
            byte[] se = (byte[]) marshaller.objectFromObjectStream(ois);
            if (trace) log.tracef("Store %s from stream in dummy store@%s", se, Util.hexIdHashCode(store));
            store.put(deserializeEntry(se).getKey(), se);
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public void toStream(ObjectOutput oos) throws CacheLoaderException {
      record("toStream");
      try {
         marshaller.objectToObjectStream(store.size(), oos);
         for (byte[] se : store.values()) marshaller.objectToObjectStream(se, oos);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   @Override
   public void clear() {
      record("clear");
      if (trace) log.trace("Clear store");
      store.clear();
   }

   @Override
   public boolean remove(Object key) {
      record("remove");
      if (store.remove(key) != null) {
         if (debug) log.debugf("Removed %s from dummy store", key);
         return true;
      }

      if (debug) log.debugf("Key %s not present in store, so don't remove", key);
      return false;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      long currentTimeMillis = System.currentTimeMillis();
      for (Iterator<byte[]> i = store.values().iterator(); i.hasNext();) {
         InternalCacheEntry se = deserializeEntry(i.next());
         if (se.isExpired(currentTimeMillis)) i.remove();
      }
   }

   @Override
   public void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache, StreamingMarshaller m) throws
         CacheLoaderException {
      super.init(configuration, cache, m);
      this.configuration = (DummyInMemoryCacheStoreConfiguration) configuration;
      storeName = this.configuration.storeName();
      if (marshaller == null) marshaller = new TestObjectStreamMarshaller();
   }

   @Override
   public InternalCacheEntry load(Object key) {
      record("load");
      if (key == null) return null;
      InternalCacheEntry se = deserializeEntry(store.get(key));
      if (se == null) return null;
      if (se.isExpired(System.currentTimeMillis())) {
         log.debugf("Key %s exists, but has expired.  Entry is %s", key, se);
         store.remove(key);
         return null;
      }

      return se;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() {
      record("loadAll");
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
      final long currentTimeMillis = System.currentTimeMillis();
      for (Iterator<byte[]> i = store.values().iterator(); i.hasNext();) {
         InternalCacheEntry se = deserializeEntry(i.next());
         if (se.isExpired(currentTimeMillis)) {
            log.debugf("Key %s exists, but has expired.  Entry is %s", se.getKey(), se);
            i.remove();
         } else
            s.add(se);
      }
      return s;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      record("load");
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>(numEntries);
      final long currentTimeMillis = System.currentTimeMillis();
      for (Iterator<byte[]> i = store.values().iterator(); i.hasNext() && s.size() < numEntries;) {
         InternalCacheEntry se = deserializeEntry(i.next());
         if (se.isExpired(currentTimeMillis)) {
            log.debugf("Key %s exists, but has expired.  Entry is %s", se.getKey(), se);
            i.remove();
         } else if (s.size() < numEntries) {
            s.add(se);
         }
      }
      return s;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      record("loadAllKeys");
      Set<Object> set = new HashSet<Object>();
      for (Object key: store.keySet()) {
         if (keysToExclude == null || !keysToExclude.contains(key)) {
            log.debugf("Load %s from store %s@%s", key, storeName, Util.hexIdHashCode(store));
            set.add(key);
         }
      }
      return set;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();


      if (store != null)
         return;

      store = new ConcurrentHashMap<Object, byte[]>();
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
      for (Method method: CacheStore.class.getMethods()) {
         m.put(method.getName(), new AtomicInteger(0));
      }
      return m;
   }

   @Override
   public void stop() throws CacheLoaderException {
      record("stop");
      super.stop();

      if (configuration.purgeOnStartup()) {
         if (storeName != null) {
            stores.remove(storeName);
         }
      }
   }

   public boolean isEmpty() {
      return store.isEmpty();
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
      long killTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < killTime) {
         InternalCacheEntry entry = deserializeEntry(store.get(key));
         if (entry != null && entry.getValue().equals(expectedValue)) return;
         TestingUtil.sleepThread(50);
      }
      throw new RuntimeException(String.format(
            "Timed out waiting (%d ms) for cache store to contain key=%s with value=%s",
            timeout, key, expectedValue));
   }

   public void blockUntilCacheStoreContains(Set<Object> expectedState, long timeout) {
      long killTime = System.currentTimeMillis() + timeout;
      // Set<? extends Map.Entry<?, InternalCacheEntry>> expectedEntries = expectedState.entrySet();
      Set<Object> notStored = null;
      Set<Object> notRemoved = null;
      while (System.currentTimeMillis() < killTime) {
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
}
