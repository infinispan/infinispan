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
package org.infinispan.jsr107.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.cache.Cache;
import javax.cache.CacheLoader;
import javax.cache.Configuration;
import javax.cache.CacheManager;
import javax.cache.CacheStatistics;
import javax.cache.ExpiryPolicy;
import javax.cache.SimpleConfiguration;
import javax.cache.Status;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryListenerRegistration;
import javax.cache.mbeans.CacheMXBean;

import org.infinispan.util.InfinispanCollections;
import org.jsr107.ri.DelegatingCacheMXBean;


/**
 * InfinispanCache is Infinispan's implementation of {@link javax.cache.Cache} interface.
 * 
 * @author Vladimir Blagojevic
 * @since 5.3
 */
public class InfinispanCache<K, V> implements Cache<K, V> {

   private final InfinispanCacheManager cacheManager;
   private final Configuration<K, V> configuration;
   private final org.infinispan.Cache<K, V> delegateCache;
   private final CacheStatistics stats;
   private final CacheMXBean mxBean;
   private final ConcurrentHashMap<CacheEntryListener<? super K, ? super V>, CacheEntryListenerRegistration<? super K, ? super V>> listeners = new ConcurrentHashMap<CacheEntryListener<? super K, ? super V>, CacheEntryListenerRegistration<? super K, ? super V>>();
   
   private final ExpiryPolicy<? super K, ? super V> expiryPolicy;
   private final ExecutorService executorService = Executors.newFixedThreadPool(1);

   public InfinispanCache(org.infinispan.Cache<K, V> delegateCache,
            InfinispanCacheManager cacheManager, ClassLoader classLoader, Configuration<K, V> c) {
      super();
      this.delegateCache = delegateCache;
      this.cacheManager = cacheManager;
      // a configuration copy as required by the spec
      this.configuration = new SimpleConfiguration<K, V>(c);
      this.mxBean = new DelegatingCacheMXBean<K, V>(this);
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
      return InfinispanStatusConverter.convert(delegateCache.getStatus());
   }

   @Override
   public void start() {
      // no op
      //TOOD need to check state before start?
      delegateCache.start();
      
      //add listener as they were wiped out on stop
      delegateCache.addListener(new InfinispanCacheListenerAdapter<K,V>(this));
   }

   @Override
   public void stop() {            
      delegateCache.stop();
   }

   @Override
   public void clear() {
      delegateCache.clear();
   }

   @Override
   public boolean containsKey(K key) {
      checkStarted();
      return delegateCache.containsKey(key);
   }

   @Override
   public V get(K key) {
      checkStarted();
      V result = delegateCache.get(key);
      return result;
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
      V prev = delegateCache.put(key, value);
      return prev;
   }

   @Override
   public V getAndRemove(K key) {
      checkStarted();
      V v = delegateCache.remove(key);
      return v;

   }

   @Override
   public V getAndReplace(K key, V value) {
      checkStarted();
      V prev = delegateCache.replace(key, value);
      return prev;
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
      return delegateCache.getName();
   }

   @Override
   public CacheStatistics getStatistics() {
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
      // TODO
      return null;
   }

   @Override
   public Iterator<javax.cache.Cache.Entry<K, V>> iterator() {
      checkStarted();
      // TODO
      return null;
   }

   @Override
   public Future<V> load(final K key) {
      checkStarted();

      final CacheLoader<K, ? extends V> cacheLoader = configuration.getCacheLoader();

      // spec required
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
                        if (!containsKey(key)) {
                           keysNotInStore.add(key);
                        }
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
      // TODO use ignore flag
      delegateCache.put(key, value);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> inputMap) {
      checkStarted();
      // spec required check
      if (inputMap == null || inputMap.containsKey(null) || inputMap.containsValue(null)) {
         throw new NullPointerException("inputMap is null or keys/values contain a null entry: "
                  + inputMap);
      }
      /**
       * TODO Similar to mentioned before, it'd be interesting to see if multiple putAsync() calls
       * could be executed in parallel to speed up.
       * 
       */
      for (java.util.Map.Entry<? extends K, ? extends V> e : inputMap.entrySet()) {
         put(e.getKey(), e.getValue());
      }

   }

   @Override
   public boolean putIfAbsent(K key, V value) {
      checkStarted();
      return delegateCache.putIfAbsent(key, value) == null;
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
      V remove = delegateCache.remove(key);
      return remove != null;
   }

   @Override
   public boolean remove(K key, V oldValue) {
      checkStarted();
      return delegateCache.remove(key, oldValue);
   }

   @Override
   public void removeAll() {
      checkStarted();
      // TODO like clear but have to notify listeners
      delegateCache.clear();
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
      V replace = delegateCache.replace(key, value);
      return replace != null;
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      checkStarted();
      return delegateCache.replace(key, oldValue, newValue);
   }

   @Override
   public boolean unregisterCacheEntryListener(CacheEntryListener<?, ?> cacheEntryListener) {
      if (cacheEntryListener == null) {
         return false;
      } else {
         return listeners.remove(cacheEntryListener) != null;
      }
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
      return delegateCache.size();
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
}
