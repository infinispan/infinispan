package org.infinispan.jcache.remote;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.management.MBeanServer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.jcache.MutableJCacheEntry;
import org.infinispan.jcache.logging.Log;

public class JCache<K, V> extends AbstractJCache<K, V> {
   private static final Log log =
         LogFactory.getLog(JCache.class, Log.class);

   private volatile boolean isClosed = false;

   private final RemoteCache<K, V> cacheWithoutStats;
   private final RemoteCache<K, V> cache;
   private final RemoteCache<K, V> cacheWithoutReturnValue;
   private final RemoteCache<K, V> cacheWithCacheStore;

   private final LocalStatistics stats;

   public JCache(RemoteCache<K, V> cache, RemoteCache<K, V> cacheForceReturnValue, CacheManager cacheManager, ConfigurationAdapter<K, V> configurationAdapter) {
      super(configurationAdapter.getConfiguration(), cacheManager, new JCacheNotifier<K, V>());

      setCacheLoader(configuration);
      setCacheWriter(configuration);

      stats = new LocalStatistics();

      this.cacheWithoutStats = new RemoteCacheWithOldValue<>(cache);

      this.cache = new RemoteCacheWithOldValue<>(new RemoteCacheWithStats<>(new RemoteCacheWithSyncListeners<>(cacheForceReturnValue, notifier, this), stats));
      this.cacheWithoutReturnValue = new RemoteCacheWithOldValue<>(new RemoteCacheWithStats<>(cache, stats));
      this.cacheWithCacheStore = new RemoteCacheWithCacheStore<K, V>(this.cache, jcacheLoader, jcacheWriter, configuration) {
         @Override
         protected void onLoad(K key, V value) {
            JCache.this.put(JCache.this.cache, JCache.this.cache, key, value, false);
         }
      };

      addConfigurationListeners();

      if (configuration.isManagementEnabled())
         setManagementEnabled(true);

      if (configuration.isStatisticsEnabled())
         setStatisticsEnabled(true);
   }

   @Override
   public V get(K key) {
      checkNotClosed();
      checkNotNull(key, "key");

      V value = cacheWithCacheStore.get(key);
      if (value != null) {
         updateTTLForAccessed(cache, key, value);
      }
      return value;
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      checkNotClosed();
      checkNotNullOrNullElement(keys, "keys");

      if (keys.isEmpty()) {
         return InfinispanCollections.emptyMap();
      }

      /**
       * TODO: Just an idea here to consider down the line: if keys.size() is big (TBD...), each of
       * this get calls could maybe be swapped by getAsync() in order to paralelise the retrieval of
       * entries. It'd be interesting to do a small performance test to see after which number of
       * elements doing it in paralel becomes more efficient than sequential :)
       */
      Map<K, V> result = new HashMap<K, V>(keys.size());
      for (K key : keys) {
         V value = get(key);
         if (value != null) {
            result.put(key, value);
         }
      }
      return result;
   }

   @Override
   public boolean containsKey(K key) {
      checkNotClosed();
      checkNotNull(key, "key");
      return cache.containsKey(key);
      //FIXME implement me
   }

   @Override
   public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
      checkNotClosed();
      checkNotNull(keys, "keys");

      if (jcacheLoader == null) {
         setListenerCompletion(completionListener);
      } else {
         loadAllFromJCacheLoader(keys, replaceExistingValues, completionListener, cache, cache);
      }
   }

   @Override
   public void put(K key, V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");

      writeToCacheWriter(key, value);
      put(cache, cacheWithoutStats, key, value, false);
      //FIXME locks
   }

   private void writeToCacheWriter(K key, V value) {
      if (!configuration.isWriteThrough() || jcacheWriter == null) {
         return;
      }
      try {
         jcacheWriter.write(new JCacheEntry<K, V>(key, value));
      } catch (Exception ex) {
         throw Exceptions.launderCacheWriterException(ex);
      }
   }

   private void removeFromCacheWriter(K key) {
      if (!configuration.isWriteThrough() || jcacheWriter == null) {
         return;
      }
      try {
         jcacheWriter.delete(key);
      } catch (Exception ex) {
         throw Exceptions.launderCacheWriterException(ex);
      }
   }

   @Override
   public V getAndPut(K key, V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");

      //FIXME locks
      V prev = put(cache, cache, key, value, false);

      writeToCacheWriter(key, value);
      return prev;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> inputMap) {
      checkNotClosed();
      checkNotNullOrNullKV(inputMap, "map");

      //FIXME locks
      for (final Map.Entry<? extends K, ? extends V> e : inputMap.entrySet()) {
         final K key = e.getKey();
         final V value = e.getValue();
         put(key, value);
      }
   }

   @Override
   public boolean putIfAbsent(K key, V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");

      boolean put = put(cache, cacheWithoutStats, key, value, true) == null;
      if (put) {
         writeToCacheWriter(key, value);
      }
      return put;
      //FIXME locks
   }

   @Override
   public boolean remove(K key) {
      checkNotClosed();
      checkNotNull(key, "key");

      removeFromCacheWriter(key);
      return cache.remove(key) != null;
      //FIXME locks
   }

   @Override
   public boolean remove(K key, V oldValue) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(oldValue, "oldValue");

      boolean removed = remove(cache, key, oldValue);
      if (removed) {
         removeFromCacheWriter(key);
      }
      return removed;
      //FIXME locks
   }

   @Override
   public V getAndRemove(K key) {
      checkNotClosed();
      checkNotNull(key, "key");

      V prev = cache.remove(key);
      removeFromCacheWriter(key);
      if (prev != null) {
         stats.incrementCacheHits();
      } else {
         stats.incrementCacheMisses();
      }

      return prev;
      //FIXME locks
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(oldValue, "oldValue");
      checkNotNull(newValue, "newValue");

      boolean replaced = replace(cache, cache, key, oldValue, newValue, true);
      if (replaced) {
         writeToCacheWriter(key, newValue);
      }
      return replaced;
      //FIXME locks
   }

   @Override
   public boolean replace(K key, V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      boolean replaced = replace(cache, cacheWithoutStats, key, null, value, false);
      if (replaced) {
         writeToCacheWriter(key, value);
      } else {
         stats.incrementCacheMisses();
      }
      return replaced;
      //FIXME locks
   }

   @Override
   public V getAndReplace(K key, V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");

      V prev = replace(cache, key, value);
      if (prev != null) {
         writeToCacheWriter(key, value);
      } else {
         stats.incrementCacheMisses();
      }

      return prev;
      //FIXME locks
   }

   @Override
   public void removeAll(Set<? extends K> keys) {
      checkNotClosed();
      checkNotNullOrNullElement(keys, "keys");
      for (K k : keys) {
         remove(k);
      }
      //FIXME implement me
   }

   @Override
   public void removeAll() {
      checkNotClosed();

      Iterator<K> it = cacheWithoutStats.keySet().iterator();
      while (it.hasNext()) {
         remove(it.next());
      }
      //FIXME locks
   }

   @Override
   public void clear() {
      checkNotClosed();

      Iterator<K> it = cacheWithoutStats.keySet().iterator();
      while (it.hasNext()) {
         cacheWithoutStats.remove(it.next());
      }
      //FIXME locks
   }

   @Override
   public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(entryProcessor, "entryProcessor");

      // Using references for backup copies to provide perceived exclusive
      // read access, and only apply changes if original value was not
      // changed by another thread, the JSR requirements for this method could
      // have been full filled. However, the TCK has some timing checks which
      // verify that under contended access, one of the threads should "wait"
      // for the other, hence the use locks.

      if (log.isTraceEnabled())
         log.tracef("Invoke entry processor %s for key=%s", entryProcessor, key);

      // Get old value skipping any listeners to impacting
      // listener invocation expectations set by the TCK.
      V oldValue = cache.get(key);

      MutableJCacheEntry<K, V> mutable = createMutableCacheEntry(oldValue, key);
      T ret = processEntryProcessor(mutable, entryProcessor, arguments);

      switch (mutable.getOperation()) {
      case NONE:
         break;
      case ACCESS:
         updateTTLForAccessed(cache, key, oldValue);
         break;
      case UPDATE:
         V newValue = mutable.getNewValue();
         if (newValue == null) {
            throw new EntryProcessorException();
         }
         if (oldValue != null) {
            // Only allow change to be applied if value has not
            // changed since the start of the processing.
            replace(cache, cacheWithoutStats, key, oldValue, newValue, true);
         } else {
            put(cache, cache, key, newValue, true);
         }
         writeToCacheWriter(key, newValue);
         break;
      case REMOVE:
         cache.remove(key);
         removeFromCacheWriter(key);
         break;
      }

      return ret;
   }

   private MutableJCacheEntry<K, V> createMutableCacheEntry(V safeOldValue, K key) {
      return new MutableJCacheEntry<K, V>(configuration.isReadThrough() ? cacheWithCacheStore : cache, cacheWithoutStats, key, safeOldValue);
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public void close() {
      //TODO
      cache.stop();
      isClosed = true;
   }

   @Override
   public boolean isClosed() {
      return isClosed;
   }

   @Override
   public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.addListener(listenerCfg, this, notifier);
      addCacheEntryListenerConfiguration(listenerCfg);
   }

   @Override
   public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.removeListener(listenerCfg, this);
      removeCacheEntryListenerConfiguration(listenerCfg);
   }

   @Override
   public Iterator<javax.cache.Cache.Entry<K, V>> iterator() {
      checkNotClosed();

      return new Itr();
   }

   @Override
   protected MBeanServer getMBeanServer() {
      return ManagementFactory.getPlatformMBeanServer();
   }

   @Override
   protected Object getCacheStatisticsMXBean() {
      return stats;
   }

   protected AbstractJCache<K, V> checkNotClosed() {
      if (isClosed())
         throw log.cacheClosed();

      return this;
   }

   private JCache<K, V> checkNotNullOrNullKV(Map<? extends K, ? extends V> map, String name) {
      checkNotNull((Object) map, name);
      if (containsNullKey(map)) {
         throw log.parameterMustNotContainNullKeys(name);
      }
      if (containsNullValue(map)) {
         throw log.parameterMustNotContainNullValues(name);
      }
      return this;
   }

   private JCache<K, V> checkNotNullOrNullElement(Collection<?> collection, String name) {
      checkNotNull((Object) collection, name);
      if (containsNull(collection)) {
         throw log.parameterMustNotContainNullKeys(name);
      }
      return this;
   }

   private boolean containsNull(Collection<?> collection) {
      if (collection == null) {
         throw new IllegalArgumentException("Argument cannot be null.");
      }
      try {
         return collection.contains(null);
      } catch (NullPointerException ex) {
         /* Collection doesn't support null elements. */
         return false;
      }
   }

   private boolean containsNullKey(Map<? extends K, ? extends V> map) {
      if (map == null) {
         throw new IllegalArgumentException("Argument cannot be null.");
      }
      try {
         return map.containsKey(null);
      } catch (NullPointerException ex) {
         /* Map doesn't support null keys. */
         return false;
      }
   }

   private boolean containsNullValue(Map<? extends K, ? extends V> map) {
      if (map == null) {
         throw new IllegalArgumentException("Argument cannot be null.");
      }
      try {
         return map.containsValue(null);
      } catch (NullPointerException ex) {
         /* Map doesn't support null values. */
         return false;
      }
   }

   private class Itr implements Iterator<Cache.Entry<K, V>> {

      private final Iterator<K> it = cache.keySet().iterator();
      private Entry<K, V> current;
      private Entry<K, V> next;

      Itr() {
         fetchNext();
      }

      private void fetchNext() {
         //TODO: fix stats
//         long start = statisticsEnabled() ? System.nanoTime() : 0;
         if (it.hasNext()) {
            K key = it.next();
            V value = cache.get(key);
            next = new JCacheEntry<K, V>(key, value);
//            if (statisticsEnabled()) {
//             stats.incrementCacheHits();
//               stats.addGetTimeNano(System.nanoTime() - start);
//            }
         } else {
            next = null;
         }
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public Entry<K, V> next() {
         if (next == null)
            fetchNext();

         if (next == null)
            throw new NoSuchElementException();

         // Set return value
         Entry<K, V> ret = next;

//         // Force expiration if needed
         updateTTLForAccessed(cache, next.getKey(), next.getValue());

         current = next;

         // Fetch next...
         fetchNext();

         return ret;
      }

      @Override
      public void remove() {
         if (current == null)
            throw new IllegalStateException();

         // TODO: Should Infinispan's core iterators be mutable?
         // It can be worked around as shown here for JSR-107 needs
         K k = current.getKey();
         current = null;
         cache.remove(k);
         removeFromCacheWriter(k);
      }
   }
   @Override
   protected void addListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.addClientListener(listenerAdapter);
   }

   @Override
   protected void removeListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.removeClientListener(listenerAdapter);
   }

   @Override
   protected void evict(K key) {
      //Nothing to do.
   }

   @Override
   protected void addCacheLoaderAdapter(CacheLoader<K, V> cacheLoader) {
      //Nothing to do.
   }

   @Override
   protected void addCacheWriterAdapter(CacheWriter<? super K, ? super V> cacheWriter) {
      //Nothing to do.
   }
}
