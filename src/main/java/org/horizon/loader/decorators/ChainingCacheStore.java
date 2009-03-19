package org.horizon.loader.decorators;

import org.horizon.Cache;
import org.horizon.loader.CacheLoader;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.modifications.Modification;
import org.horizon.marshall.Marshaller;

import javax.transaction.Transaction;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A chaining cache loader that allows us to configure > 1 cache loader.
 * <p/>
 * READ operations are directed to each of the cache loaders (in the order which they were configured) until a non-null
 * (or non-empty in the case of retrieving collection objects) result is achieved.
 * <p/>
 * WRITE operations are propagated to ALL registered cache stores specified, that set ignoreModifications to false.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ChainingCacheStore implements CacheStore {

   // linked hash sets used since it provides fast (O(1)) iteration, maintains order and provides O(1) lookups to values as well.
   LinkedHashMap<CacheLoader, CacheLoaderConfig> loaders = new LinkedHashMap<CacheLoader, CacheLoaderConfig>();
   LinkedHashMap<CacheStore, CacheLoaderConfig> stores = new LinkedHashMap<CacheStore, CacheLoaderConfig>();

   public void store(StoredEntry ed) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.store(ed);
   }

   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
      for (Map.Entry<CacheStore, CacheLoaderConfig> e : stores.entrySet()) {
         if (e.getValue().isFetchPersistentState()) {
            e.getKey().fromStream(inputStream);
            // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
            break;
         }
      }
   }

   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      // loading and storing state via streams is *only* supported on the *first* store that has fetchPersistentState set.
      for (Map.Entry<CacheStore, CacheLoaderConfig> e : stores.entrySet()) {
         if (e.getValue().isFetchPersistentState()) {
            e.getKey().toStream(outputStream);
            // do NOT continue this for other stores, since the stream will not be in an appropriate state anymore
            break;
         }
      }
   }

   public void clear() throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.clear();
   }

   public boolean remove(Object key) throws CacheLoaderException {
      boolean r = false;
      for (CacheStore s : stores.keySet()) r = s.remove(key) || r;
      return r;
   }

   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.removeAll(keys);
   }

   public void purgeExpired() throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.purgeExpired();
   }

   public void commit(Transaction tx) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.commit(tx);
   }

   public void rollback(Transaction tx) {
      for (CacheStore s : stores.keySet()) s.rollback(tx);
   }

   public void prepare(List<? extends Modification> list, Transaction tx, boolean isOnePhase) throws CacheLoaderException {
      for (CacheStore s : stores.keySet()) s.prepare(list, tx, isOnePhase);
   }

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      for (Map.Entry<CacheLoader, CacheLoaderConfig> e : loaders.entrySet()) {
         e.getKey().init(e.getValue(), cache, m);
      }
   }

   public StoredEntry load(Object key) throws CacheLoaderException {
      StoredEntry se = null;
      for (CacheLoader l : loaders.keySet()) {
         se = l.load(key);
         if (se != null) break;
      }
      return se;
   }

   public Set<StoredEntry> loadAll() throws CacheLoaderException {
      Set<StoredEntry> set = new HashSet<StoredEntry>();
      for (CacheStore s : stores.keySet()) set.addAll(s.loadAll());
      return set;
   }

   public boolean containsKey(Object key) throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) {
         if (l.containsKey(key)) return true;
      }
      return false;
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;
   }

   public void start() throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) l.start();
   }

   public void stop() throws CacheLoaderException {
      for (CacheLoader l : loaders.keySet()) l.stop();
   }

   public void addCacheLoader(CacheLoader loader, CacheLoaderConfig config) {
      loaders.put(loader, config);
      if (loader instanceof CacheStore) stores.put((CacheStore) loader, config);
   }

   public void purgeIfNecessary() throws CacheLoaderException {
      for (Map.Entry<CacheStore, CacheLoaderConfig> e : stores.entrySet()) {
         if (e.getValue().isPurgeOnStartup()) e.getKey().clear();
      }
   }
}
