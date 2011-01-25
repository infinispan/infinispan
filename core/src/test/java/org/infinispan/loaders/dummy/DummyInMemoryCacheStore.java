package org.infinispan.loaders.dummy;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DummyInMemoryCacheStore extends AbstractCacheStore {
   private static final Log log = LogFactory.getLog(DummyInMemoryCacheStore.class);
   private static final boolean trace = log.isTraceEnabled(); 
   static final ConcurrentMap<String, Map<Object, InternalCacheEntry>> stores = new ConcurrentHashMap<String, Map<Object, InternalCacheEntry>>();
   static final ConcurrentMap<String, Map<String, Integer>> storeStats = new ConcurrentHashMap<String, Map<String, Integer>>();
   String storeName = "__DEFAULT_STORES__";
   Map<Object, InternalCacheEntry> store;
   Map<String, Integer> stats;
   Cfg config;


   private void record(String method) {
      int i = stats.get(method);
      stats.put(method, i + 1);
   }

   public void store(InternalCacheEntry ed) {
      record("store");
      if (ed != null) {
         if (trace) log.trace("Store %s in dummy map store@%s", ed, Integer.toHexString(System.identityHashCode(store)));
         config.failIfNeeded(ed.getKey());
         store.put(ed.getKey(), ed);
      }
   }

   @SuppressWarnings("unchecked")
   public void fromStream(ObjectInput ois) throws CacheLoaderException {
      record("fromStream");
      try {
         int numEntries = (Integer) marshaller.objectFromObjectStream(ois);
         for (int i = 0; i < numEntries; i++) {
            InternalCacheEntry e = (InternalCacheEntry) marshaller.objectFromObjectStream(ois);
            if (trace) log.trace("Store %s from stream in dummy store@%s", e, Integer.toHexString(System.identityHashCode(store)));
            store.put(e.getKey(), e);
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   public void toStream(ObjectOutput oos) throws CacheLoaderException {
      record("toStream");
      try {
         marshaller.objectToObjectStream(store.size(), oos);
         for (InternalCacheEntry se : store.values()) marshaller.objectToObjectStream(se, oos);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   public void clear() {
      record("clear");
      if (trace) log.trace("Clear store");
      store.clear();
   }

   public boolean remove(Object key) {
      record("remove");
      if (store.remove(key) != null) {
         if (trace) log.trace("Removed %s from dummy store", key);
         return true;
      }

      if (trace) log.trace("Key %s not present in store, so don't remove", key);
      return false;
   }

   protected void purgeInternal() throws CacheLoaderException {
      for (Iterator<InternalCacheEntry> i = store.values().iterator(); i.hasNext();) {
         InternalCacheEntry se = i.next();
         if (se.isExpired()) i.remove();
      }
   }

   public void init(CacheLoaderConfig config, Cache cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (Cfg) config;
      if (marshaller == null) marshaller = new TestObjectStreamMarshaller();
   }

   public InternalCacheEntry load(Object key) {
      record("load");
      if (key == null) return null;
      InternalCacheEntry se = store.get(key);
      if (se == null) return null;
      if (se.isExpired()) {
         log.debug("Key %s exists, but has expired.  Entry is %s", key, se);
         store.remove(key);
         return null;
      }

      return se;
   }

   public Set<InternalCacheEntry> loadAll() {
      record("loadAll");
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
      for (Iterator<InternalCacheEntry> i = store.values().iterator(); i.hasNext();) {
         InternalCacheEntry se = i.next();
         if (se.isExpired()) {
            log.debug("Key %s exists, but has expired.  Entry is %s", se.getKey(), se);
            i.remove();
         } else
            s.add(se);
      }
      return s;
   }

   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      record("load");
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>(numEntries);
      for (Iterator<InternalCacheEntry> i = store.values().iterator(); i.hasNext() && s.size() < numEntries;) {
         InternalCacheEntry se = i.next();
         if (se.isExpired()) {
            log.debug("Key %s exists, but has expired.  Entry is %s", se.getKey(), se);
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
         if (keysToExclude == null || !keysToExclude.contains(key)) set.add(key);
      }
      return set;
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      record("getConfigurationClass");
      return Cfg.class;
   }

   public void start() throws CacheLoaderException {
      super.start();
      storeName = config.getStore();
      if (cache != null) storeName += "_" + cache.getName();

      Map<Object, InternalCacheEntry> m = new ConcurrentHashMap<Object, InternalCacheEntry>();
      Map<Object, InternalCacheEntry> existing = stores.putIfAbsent(storeName, m);
      store = existing == null ? m : existing;

      Map<String, Integer> s = newStatsMap();
      Map<String, Integer> existingStats = storeStats.putIfAbsent(storeName, s);
      stats = existingStats == null ? s : existingStats;

      // record at the end!
      record("start");
   }

   private Map<String, Integer> newStatsMap() {
      Map<String, Integer> m = new ConcurrentHashMap<String, Integer>();
      for (Method method: CacheStore.class.getMethods()) {
         m.put(method.getName(), 0);
      }
      return m;
   }

   public void stop() {
      record("stop");
      if (config.isCleanBetweenRestarts()) {
         stores.remove(config.getStore());
      }
   }

   public boolean isEmpty() {
      return store.isEmpty();
   }

   public Map<String, Integer> stats() {
      return Collections.unmodifiableMap(stats);
   }

   public void clearStats() {
      for (String k: stats.keySet()) stats.put(k, 0);
   }

   public static class Cfg extends AbstractCacheStoreConfig {

      private static final long serialVersionUID = 4258914047690999424L;
      
      boolean debug;
      String store = "__DEFAULT_STORE__";
      boolean cleanBetweenRestarts = true;
      private Object failKey;

      public Cfg() {
         setCacheLoaderClassName(DummyInMemoryCacheStore.class.getName());
      }

      public Cfg(String name) {
         this();
         setStore(name);
      }

      public Cfg(String name, boolean cleanBetweenRestarts) {
         this(name);
         this.cleanBetweenRestarts = cleanBetweenRestarts;
      }

      public Cfg(boolean cleanBetweenRestarts) {
         setCacheLoaderClassName(DummyInMemoryCacheStore.class.getName());
         this.cleanBetweenRestarts = cleanBetweenRestarts;
      }

      public boolean isDebug() {
         return debug;
      }

      public void setDebug(boolean debug) {
         this.debug = debug;
      }

      public String getStore() {
         return store;
      }

      public void setStore(String store) {
         this.store = store;
      }

      @Override
      public Cfg clone() {
         return (Cfg) super.clone();
      }

      public boolean isCleanBetweenRestarts() {
         return cleanBetweenRestarts;
      }

      public void setFailKey(Object failKey) {
         this.failKey = failKey;
      }

      public void failIfNeeded(Object key) {
         if(failKey != null && failKey.equals(key)) throw new RuntimeException("Induced failure on key:" + key);
      }
   }
}
