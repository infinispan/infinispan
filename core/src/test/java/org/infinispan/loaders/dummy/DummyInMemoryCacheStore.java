/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.dummy;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.Util;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@CacheLoaderMetadata(configurationClass = DummyInMemoryCacheStore.Cfg.class)
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
   Cfg config;

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
      if (ed != null) {
         if (debug) log.debugf("Store %s in dummy map store@%s", ed, Util.hexIdHashCode(store));
         config.failIfNeeded(ed.getKey());
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
   @SuppressWarnings("unchecked")
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
   public void init(CacheLoaderConfig config, Cache cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (Cfg) config;
      storeName = this.config.getStoreName();
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
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      record("getConfigurationClass");
      return Cfg.class;
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

      if (config.isPurgeOnStartup()) {
         String storeName = config.getStoreName();
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

   public static class Cfg extends AbstractCacheStoreConfig {

      private static final long serialVersionUID = 4258914047690999424L;

      boolean debug;
      String storeName = null;
      private Object failKey;

      public Cfg() {
         this(null);
      }

      public Cfg(String name) {
         setCacheLoaderClassName(DummyInMemoryCacheStore.class.getName());
         storeName(name);
      }

      public boolean isDebug() {
         return debug;
      }

      /**
       * @deprecated use {@link #debug(boolean)}
       */
      @Deprecated
      public void setDebug(boolean debug) {
         this.debug = debug;
      }

      public Cfg debug(boolean debug) {
         setDebug(debug);
         return this;
      }

      public String getStoreName() {
         return storeName;
      }

      /**
       * @deprecated use {@link #storeName(String)}
       */
      @Deprecated
      public void setStoreName(String store) {
         this.storeName = store;
      }

      public Cfg storeName(String store) {
         setStoreName(store);
         return this;
      }

      @Override
      public Cfg clone() {
         return (Cfg) super.clone();
      }

      /**
       * @deprecated use {@link #failKey(Object)}
       */
      @Deprecated
      public void setFailKey(Object failKey) {
         this.failKey = failKey;
      }

      public Cfg failKey(Object failKey) {
         setFailKey(failKey);
         return this;
      }

      public void failIfNeeded(Object key) {
         if(failKey != null && failKey.equals(key)) throw new RuntimeException("Induced failure on key:" + key);
      }

      @Override
      public Cfg fetchPersistentState(Boolean fetchPersistentState) {
         super.fetchPersistentState(fetchPersistentState);
         return this;
      }

      @Override
      public Cfg ignoreModifications(Boolean ignoreModifications) {
         super.ignoreModifications(ignoreModifications);
         return this;
      }

      @Override
      public Cfg purgeOnStartup(Boolean purgeOnStartup) {
         super.purgeOnStartup(purgeOnStartup);
         return this;
      }

      @Override
      public Cfg purgerThreads(Integer purgerThreads) {
         super.purgerThreads(purgerThreads);
         return this;
      }

      @Override
      public Cfg purgeSynchronously(Boolean purgeSynchronously) {
         super.purgeSynchronously(purgeSynchronously);
         return this;
      }

      @Override
      public Properties getProperties() {
         Properties p = super.getProperties();
         p.setProperty("debug", Boolean.toString(debug));
         if (storeName != null)
            p.setProperty("storeName", storeName);
         if (failKey != null) // TODO: Find a better way...
            p.setProperty("storeName", failKey.toString());
         return p;
      }
   }
}
