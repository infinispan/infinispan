/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.jcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import javax.cache.*;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryListenerRegistration;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.util.InfinispanCollections;

/**
 * Infinispan's implementation of {@link javax.cache.Cache} interface.
 * 
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCache<K, V> implements Cache<K, V> {

   private final JCacheManager cacheManager;
   private final Configuration<K, V> configuration;
   private final AdvancedCache<K, V> cache;
   private final AdvancedCache<K, V> ignoreReturnValuesCache;
   private final AdvancedCache<K, V> skipCacheLoadCache;
   private final CacheStatisticsMXBean stats;
   private final CacheMXBean mxBean;
   private final ConcurrentHashMap<CacheEntryListener<? super K, ? super V>, CacheEntryListenerRegistration<? super K, ? super V>> listeners = new ConcurrentHashMap<CacheEntryListener<? super K, ? super V>, CacheEntryListenerRegistration<? super K, ? super V>>();
   
   private final ExpiryPolicy<? super K, ? super V> expiryPolicy;
   private final ExecutorService executorService = Executors.newFixedThreadPool(1);

   public JCache(AdvancedCache<K, V> cache,
         JCacheManager cacheManager, Configuration<K, V> c) {
      super();
      this.cache = cache;
      this.ignoreReturnValuesCache = cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.skipCacheLoadCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);
      this.cacheManager = cacheManager;
      // a configuration copy as required by the spec
      this.configuration = new SimpleConfiguration<K, V>(c);
      this.mxBean = new RIDelegatingCacheMXBean<K, V>(this);
      this.stats = new RICacheStatistics(this);
      this.expiryPolicy = configuration.getExpiryPolicy();
      
      for (CacheEntryListenerRegistration<? super K, ? super V> r : c
               .getCacheEntryListenerRegistrations()) {

         RICacheEntryListenerRegistration<K, V> lr = new RICacheEntryListenerRegistration<K, V>(
                  r.getCacheEntryListener(), r.getCacheEntryFilter(), r.isOldValueRequired(),
                  r.isSynchronous());

         listeners.put(r.getCacheEntryListener(), lr);
      }            
   }
   
   public Map<CacheEntryListener<? super K, ? super V>, CacheEntryListenerRegistration<? super K, ? super V>> getListeners(){
      return listeners;
   }

   @Override
   public Status getStatus() {
      return JStatusConverter.convert(cache.getStatus());
   }

   @Override
   public void start() {
      // no op
      //TOOD need to check state before start?
      cache.start();
      
      //add listener as they were wiped out on stop
      cache.addListener(new JCacheListenerAdapter<K, V>(this));
   }

   @Override
   public void stop() {            
      cache.stop();
   }

   @Override
   public void clear() {
      cache.clear();
   }

   @Override
   public boolean containsKey(K key) {
      checkStarted();
      return skipCacheLoadCache.containsKey(key);
   }

   @Override
   public V get(K key) {
      checkStarted();
      V value = cache.get(key);
      if (value != null)
         updateTTLForAccessed(cache,
               new JCacheEntry<K, V>(key, value));

      return value;
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      checkStarted();
      verifyKeys(keys);
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
   public V getAndPut(K key, V value) {
      checkStarted();
      return put(cache, key, value, false);
   }

   @Override
   public V getAndRemove(K key) {
      checkStarted();
      return skipCacheLoadCache.remove(key);
   }

   @Override
   public V getAndReplace(K key, V value) {
      checkStarted();
      return skipCacheLoadCache.replace(key, value);
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }

   @Override
   public Configuration<K, V> getConfiguration() {
      return configuration;
   }

   @Override
   public CacheMXBean getMBean() {
      return mxBean;
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public CacheStatisticsMXBean getStatistics() {
      checkStarted();
      if (configuration.isStatisticsEnabled()) {
          return stats;
      } else {
          return null;
      }
   }

   @Override
   public <T> T invokeEntryProcessor(K key, EntryProcessor<K, V, T> entryProcessor) {
      checkStarted();

      // TODO: We've no unlock, and this is supposed be executable without transactions
      // TODO: We force a transaction somehow? What if no transaction config is available?
      // TODO: Spec says exclusive lock should be on reads too...

      return entryProcessor.process(
            new MutableJCacheEntry<K, V>(cache, key, cache.get(key)));
   }

   @Override
   public Iterator<Cache.Entry<K, V>> iterator() {
      checkStarted();
      return new Itr();
   }

   @Override
   public Future<V> load(final K key) {
      checkStarted();

      final CacheLoader<K, ? extends V> cacheLoader = configuration.getCacheLoader();

      // Spec required, needs to be done even if no cache loader configured
      verifyKey(key);

      // Spec required
      if (cacheLoader == null || containsKey(key)) {
         return null;
      }

      FutureTask<V> task = new FutureTask<V>(new Callable<V>() {
         @Override
         public V call() throws Exception {
            Entry<K, ? extends V> entry = cacheLoader.load(key);
            put(entry.getKey(), entry.getValue());
            return entry.getValue();
         }
      });
      executorService.submit(task);
      return task;
   }

   @Override
   public Future<Map<K, ? extends V>> loadAll(final Set<? extends K> keys) {
      checkStarted();
      verifyKeys(keys);
      final CacheLoader<K, ? extends V> cacheLoader = configuration.getCacheLoader();

      if (cacheLoader == null) {
         return null;
      }

      FutureTask<Map<K, ? extends V>> task = new FutureTask<Map<K, ? extends V>>(
         new Callable<Map<K, ? extends V>>() {
            @Override
            public Map<K, ? extends V> call() throws Exception {
               ArrayList<K> keysNotInStore = new ArrayList<K>();
               for (K key : keys) {
                  if (!containsKey(key))
                     keysNotInStore.add(key);
               }
               Map<K, ? extends V> value = cacheLoader.loadAll(keysNotInStore);
               putAll(value);
               return value;
            }
         });
      executorService.submit(task);
      return task;
   }

   @Override
   public void put(K key, V value) {
      checkStarted();
      put(ignoreReturnValuesCache, key, value, false);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> inputMap) {
      checkStarted();
      // spec required check
      if (inputMap == null || inputMap.containsKey(null) || inputMap.containsValue(null)) {
         throw new NullPointerException(
            "inputMap is null or keys/values contain a null entry: " + inputMap);
      }
      /**
       * TODO Similar to mentioned before, it'd be interesting to see if multiple putAsync() calls
       * could be executed in parallel to speed up.
       * 
       */
      for (Map.Entry<? extends K, ? extends V> e : inputMap.entrySet())
         put(ignoreReturnValuesCache, e.getKey(), e.getValue(), false);
   }

   @Override
   public boolean putIfAbsent(K key, V value) {
      checkStarted();
      return put(skipCacheLoadCache, key, value, true) == null;
   }

   @Override
   public boolean registerCacheEntryListener(
            CacheEntryListener<? super K, ? super V> cacheEntryListener, boolean requireOldValue,
            CacheEntryEventFilter<? super K, ? super V> cacheEntryFilter, boolean synchronous) {
      if (cacheEntryListener == null) {
         throw new CacheEntryListenerException("A listener may not be null");
      }
      RICacheEntryListenerRegistration<K, V> registration = new RICacheEntryListenerRegistration<K, V>(
               cacheEntryListener, cacheEntryFilter, requireOldValue, synchronous);
      return listeners.putIfAbsent(cacheEntryListener, registration) != null;
   }

   @Override
   public boolean remove(K key) {
      checkStarted();
      V remove = cache.remove(key);
      return remove != null;
   }

   @Override
   public boolean remove(K key, V oldValue) {
      checkStarted();
      return cache.remove(key, oldValue);
   }

   @Override
   public void removeAll() {
      checkStarted();
      // Calling cache.clear() won't work since there's currently no way to
      // for an Infinispan cache store to figure out all keys store and pass
      // them to CacheWriter.deleteAll(), hence, delete individually.
      // TODO: What happens with entries only in store but not in memory?

      // Delete asynchronously and then wait for removals to complete
      List<Future<V>> futures = new ArrayList<Future<V>>();
      for (Cache.Entry<K, V> entry : this)
         futures.add(cache.removeAsync(entry.getKey()));

      for (Future<V> future : futures) {
         try {
            future.get(10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(
                  "Interrupted while waiting for remove to complete");
         } catch (Exception e) {
            throw new CacheException(
                  "Removing all entries from cache failed", e);
         }
      }
   }

   @Override
   public void removeAll(Set<? extends K> keys) {
      checkStarted();
      // TODO remove but notify listeners
      verifyKeys(keys);
      for (K k : keys) {
         remove(k);
      }
   }

   @Override
   public boolean replace(K key, V value) {
      checkStarted();
      return replace(skipCacheLoadCache, key, null, value, false);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      checkStarted();
      return replace(skipCacheLoadCache, key, oldValue, newValue, true);
   }

   @Override
   public boolean unregisterCacheEntryListener(CacheEntryListener<?, ?> cacheEntryListener) {
      return cacheEntryListener != null
            && listeners.remove(cacheEntryListener) != null;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      if (clazz.isAssignableFrom(this.getClass())) {
         return clazz.cast(this);
      } else {
         throw new IllegalArgumentException("Unwrapping to type " + clazz + " failed ");
      }
   }

   public long size() {
      return cache.size();
   }

   private void checkStarted() {
      if (!getStatus().equals(Status.STARTED)) {
         throw new IllegalStateException("Cache is in " + getStatus() + " state");
      }
   }

   private void verifyKeys(Set<? extends K> keys) {
      // spec required
      if (keys == null || keys.contains(null)) {
         throw new NullPointerException("keys is null or keys contains a null: " + keys);
      }
   }

   private void verifyKey(K key) {
      // spec required
      if (key == null)
         throw new NullPointerException("Key cannot be null");
   }

   private void verifyNewValue(V newValue) {
      if (newValue == null)
         throw new NullPointerException(
               "New value cannot be null");
   }

   private void verifyOldValue(V oldValue) {
      if (oldValue == null)
         throw new NullPointerException(
               "Old value cannot be null");
   }

   private V put(AdvancedCache<K, V> cache, K key, V value,
         boolean isPutIfAbsent) {
      boolean isCreated = !cache.containsKey(key);
      V ret;
      Configuration.Duration ttl;
      Entry<K, V> entry = new JCacheEntry<K, V>(key, value);
      if (isCreated) {
         ttl = expiryPolicy.getTTLForCreatedEntry(entry);
      } else {
         // TODO: Retrieve existing lifespan setting for entry from internal container?
         ttl = expiryPolicy.getTTLForModifiedEntry(entry, null);
      }

      if (ttl == null || ttl.isEternal()) {
         ret = isPutIfAbsent
               ? cache.putIfAbsent(key, value)
               : cache.put(key, value);
      } else if (ttl.equals(Configuration.Duration.ZERO)) {
         // TODO: Can this be avoided?
         // Special case for ZERO because the Infinispan remove()
         // implementation returns true if entry was expired in the removal
         // (since it was previously stored). JSR-107 TCK expects that if
         // ZERO is passed, the entry is not stored and removal returns false.
         // So, if entry is created, do not store it in the cache.
         // If the entry is modified, explicitly remove it.
         if (!isCreated)
            ret = cache.remove(key);
         else
            ret = null;
      } else {
         long duration = ttl.getDurationAmount();
         TimeUnit timeUnit = ttl.getTimeUnit();
         ret = isPutIfAbsent
               ? cache.putIfAbsent(key, value, duration, timeUnit)
               : cache.put(key, value, duration, timeUnit);
      }

      return ret;
   }

   @SuppressWarnings("unchecked") // Varargs as generics should be supportable
   private boolean replace(AdvancedCache<K, V> cache,
         K key, V oldValue, V value, boolean isConditional) {
      boolean exists = cache.containsKey(key);
      if (exists) {
         Entry<K, V> entry = new JCacheEntry<K, V>(key, value);
         // TODO: Retrieve existing lifespan setting for entry from internal container?
         Configuration.Duration ttl = expiryPolicy
               .getTTLForModifiedEntry(entry, null);

         if (ttl == null || ttl.isEternal()) {
            return isConditional
                  ? cache.replace(key, oldValue, value)
                  : cache.replace(key, value) != null;
         } else if (ttl.equals(Configuration.Duration.ZERO)) {
            // TODO: Can this be avoided?
            // Remove explicitly
            return cache.remove(key) != null;
         } else {
            long duration = ttl.getDurationAmount();
            TimeUnit timeUnit = ttl.getTimeUnit();
            return isConditional
                  ? cache.replace(key, oldValue, value, duration, timeUnit)
                  : cache.replace(key, value, duration, timeUnit) != null;
         }
      }

      if (isConditional) {
         // Even if replace fails, values have to be validated (required by TCK)
         verifyOldValue(oldValue);
      }

      verifyNewValue(value);
      return false;
   }

   private void updateTTLForAccessed(AdvancedCache<K, V> cache, Entry<K, V> entry) {
      // TODO: Retrieve existing maxIdle setting for entry from internal container?
      Configuration.Duration ttl =
            expiryPolicy.getTTLForAccessedEntry(entry, null);

      if (ttl != null) {
         if (ttl.equals(Configuration.Duration.ZERO)) {
            // TODO: Expiry of 0 does not seem to remove entry when next accessed.
            // Hence, explicitly removing the entry.
            cache.remove(entry.getKey());
         } else {
            // The expiration policy could potentially return different values
            // every time, so don't think we can rely on maxIdle.
            long durationAmount = ttl.getDurationAmount();
            TimeUnit timeUnit = ttl.getTimeUnit();
            cache.put(entry.getKey(), entry.getValue(), durationAmount, timeUnit);
         }
      }
   }

   private class Itr implements Iterator<Cache.Entry<K, V>> {

      private final Iterator<Map.Entry<K, V>> it = cache.entrySet().iterator();
      private Entry<K, V> current;
      private Entry<K, V> next;

      Itr() {
         fetchNext();
      }

      private void fetchNext() {
         if (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            next = new JCacheEntry<K, V>(
                  entry.getKey(), entry.getValue());
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

         // Force expiration if needed
         updateTTLForAccessed(cache, next);

         current = next;

         // Fetch next...
         fetchNext();

         return ret;
      }

      @Override
      public void remove() {
         if (current == null)
            throw new IllegalStateException();

         K k = current.getKey();
         current = null;
         cache.remove(k);
      }
   }

}
