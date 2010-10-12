package org.infinispan.loaders.dummy;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DummyInMemoryCacheStore extends AbstractCacheStore {
   private static final Log log = LogFactory.getLog(DummyInMemoryCacheStore.class);
   private static final boolean trace = log.isTraceEnabled(); 
   static final ConcurrentMap<String, Map> stores = new ConcurrentHashMap<String, Map>();
   String storeName = "__DEFAULT_STORES__";
   Map<Object, InternalCacheEntry> store;
   Cfg config;

   public void store(InternalCacheEntry ed) {
      if (ed != null) {
         if (trace) log.trace("Store {0} in dummy map store@{1}", ed, Integer.toHexString(System.identityHashCode(store)));
         config.failIfNeeded(ed.getKey());
         store.put(ed.getKey(), ed);
      }
   }

   @SuppressWarnings("unchecked")
   public void fromStream(ObjectInput ois) throws CacheLoaderException {
      try {
         int numEntries = (Integer) marshaller.objectFromObjectStream(ois);
         for (int i = 0; i < numEntries; i++) {
            InternalCacheEntry e = (InternalCacheEntry) marshaller.objectFromObjectStream(ois);
            if (trace) log.trace("Store {0} from stream in dummy store@{1}", e, Integer.toHexString(System.identityHashCode(store)));
            store.put(e.getKey(), e);
         }
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   public void toStream(ObjectOutput oos) throws CacheLoaderException {
      try {
         marshaller.objectToObjectStream(store.size(), oos);
         for (InternalCacheEntry se : store.values()) marshaller.objectToObjectStream(se, oos);
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   public void clear() {
      if (trace) log.trace("Clear store");
      store.clear();
   }

   public boolean remove(Object key) {
      if (store.remove(key) != null) {
         if (trace) log.trace("Removed {0} from dummy store", key);
         return true;
      }

      if (trace) log.trace("Key {0} not present in store, so don't remove", key);
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
      if (key == null) return null;
      InternalCacheEntry se = store.get(key);
      if (se == null) return null;
      if (se.isExpired()) {
         log.debug("Key {0} exists, but has expired.  Entry is {1}", key, se);
         store.remove(key);
         return null;
      }

      return se;
   }

   public Set<InternalCacheEntry> loadAll() {
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
      for (Iterator<InternalCacheEntry> i = store.values().iterator(); i.hasNext();) {
         InternalCacheEntry se = i.next();
         if (se.isExpired()) {
            log.debug("Key {0} exists, but has expired.  Entry is {1}", se.getKey(), se);
            i.remove();
         } else
            s.add(se);
      }
      return s;
   }

   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>(numEntries);
      for (Iterator<InternalCacheEntry> i = store.values().iterator(); i.hasNext() && s.size() < numEntries;) {
         InternalCacheEntry se = i.next();
         if (se.isExpired()) {
            log.debug("Key {0} exists, but has expired.  Entry is {1}", se.getKey(), se);
            i.remove();
         } else if (s.size() < numEntries) {
            s.add(se);
         }
      }
      return s;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> set = new HashSet<Object>();
      for (Object key: store.keySet()) {
         if (keysToExclude == null || !keysToExclude.contains(key)) set.add(key);
      }
      return set;
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return Cfg.class;
   }

   @SuppressWarnings("unchecked")
   public void start() throws CacheLoaderException {
      super.start();
      storeName = config.getStore();
      if (cache != null) storeName += "_" + cache.getName();
      Map m = new ConcurrentHashMap();
      Map existing = stores.putIfAbsent(storeName, m);
      store = existing == null ? m : existing;
   }

   public void stop() {
      if (config.isCleanBetweenRestarts()) {
         stores.remove(config.getStore());
      }
   }

   public boolean isEmpty() {
      return store.isEmpty();
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
