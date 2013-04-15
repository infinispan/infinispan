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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
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
import org.infinispan.jcache.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of {@link javax.cache.Cache} interface.
 * 
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCache<K, V> implements Cache<K, V> {

   private static final Log log =
         LogFactory.getLog(JCache.class, Log.class);

   private final JCacheManager cacheManager;
   private final Configuration<K, V> configuration;
   private final AdvancedCache<K, V> cache;
   private final AdvancedCache<K, V> ignoreReturnValuesCache;
   private final AdvancedCache<K, V> skipCacheLoadCache;
   private final AdvancedCache<K, V> containsKeyCache;
   private final AdvancedCache<K, V> skipListenerCache;
   private final CacheStatisticsMXBean stats;
   private final CacheMXBean mxBean;
   private final JCacheNotifier<K, V> notifier;

   private final ExpiryPolicy<? super K, ? super V> expiryPolicy;
   private final ExecutorService executorService = Executors.newFixedThreadPool(1);
   private final LockContainer processorLocks = new ReentrantPerEntryLockContainer(32);
   private final long lockTimeout; // milliseconds

   public JCache(AdvancedCache<K, V> cache, JCacheManager cacheManager,
         JCacheNotifier<K, V> notifier,  Configuration<K, V> c) {
      super();
      this.cache = cache;
      this.ignoreReturnValuesCache = cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.skipCacheLoadCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);

      // Typical use cases of the SKIP_LISTENER_NOTIFICATION is when trying
      // to comply with specifications such as JSR-107, which mandate that
      // {@link Cache#containsKey(Object)}} calls do not fire entry visited
      // notifications, while maintaining the same behaviour that Infinispan
      // has done in the past.
      this.containsKeyCache = skipCacheLoadCache
            .withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
      this.skipListenerCache = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION);

      this.cacheManager = cacheManager;
      // a configuration copy as required by the spec
      this.configuration = new SimpleConfiguration<K, V>(c);
      this.mxBean = new RIDelegatingCacheMXBean<K, V>(this);
      this.stats = new RICacheStatistics(this);
      this.expiryPolicy = configuration.getExpiryPolicy();
      this.lockTimeout =  cache.getCacheConfiguration()
            .locking().lockAcquisitionTimeout();
      this.notifier = notifier;
      for (CacheEntryListenerRegistration<? super K, ? super V> r
            : c.getCacheEntryListenerRegistrations()) {
         notifier.addListener(r);
      }
   }
   
   @Override
   public Status getStatus() {
      return JStatusConverter.convert(cache.getStatus());
   }

   @Override
   public void start() {
      // no op
      // TODO need to check state before start?
      cache.start();
      
      // Add listener as they were wiped out on stop
      // TODO: Why not add listener only when a listener is actually registered?
      cache.addListener(new JCacheListenerAdapter<K, V>(this, notifier));
   }

   @Override
   public void stop() {            
      cache.stop();
   }

   @Override
   public void clear() {
      // TCK expects clear() to not fire any remove events
      skipListenerCache.clear();
   }

   @Override
   public boolean containsKey(final K key) {
      checkStarted();

      if (log.isTraceEnabled())
         log.tracef("Invoke containsKey(key=%s)", key);

      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return containsKeyCache.containsKey(key);
            }
         });
      }

      return containsKeyCache.containsKey(key);
   }

   @Override
   public V get(final K key) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return doGet(key);
            }
         });
      }

      return doGet(key);
   }

   private V doGet(K key) {
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
   public V getAndPut(final K key, final V value) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return put(cache, cache, key, value, false);
            }
         });
      }
      //
      return put(cache, cache, key, value, false);
   }

   @Override
   public V getAndRemove(final K key) {
      checkStarted();
      // Dummy cache.get call to force a cache entry read event
      // to be fired if the entry exists in the cache.
      skipCacheLoadCache.get(key);
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return skipCacheLoadCache.remove(key);
            }
         });
      }

      return skipCacheLoadCache.remove(key);
   }

   @Override
   public V getAndReplace(final K key, final V value) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return replace(skipCacheLoadCache, key, value);
            }
         });
      }

      return replace(skipCacheLoadCache, key, value);
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
   public <T> T invokeEntryProcessor(
         final K key, final EntryProcessor<K, V, T> entryProcessor) {
      checkStarted();

      // spec required null checks
      verifyKey(key);
      if (entryProcessor == null)
         throw new NullPointerException("Entry processor cannot be null");

      // Using references for backup copies to provide perceived exclusive
      // read access, and only apply changes if original value was not
      // changed by another thread, the JSR requirements for this method could
      // have been full filled. However, the TCK has some timing checks which
      // verify that under contended access, one of the threads should "wait"
      // for the other, hence the use locks.

      if (log.isTraceEnabled())
         log.tracef("Invoke entry processor %s for key=%s", entryProcessor, key);

      return new WithProcessorLock<T>().call(key, new Callable<T>() {
         @Override
         public T call() throws Exception {
            // Get old value skipping any listeners to impacting
            // listener invocation expectations set by the TCK.
            V oldValue = skipListenerCache.get(key);
            V safeOldValue = oldValue;
            if (configuration.isStoreByValue()) {
               // Make a copy because the entry processor could make changes
               // directly in the value, and we wanna keep a safe old value
               // around for when calling the atomic replace() call.
               safeOldValue = safeCopy(oldValue);
            }

            MutableJCacheEntry<K, V> mutable =
                  new MutableJCacheEntry<K, V>(cache, key, safeOldValue);
            T ret = entryProcessor.process(mutable);

            if (mutable.isRemoved()) {
               cache.remove(key);
            } else {
               V newValue = mutable.getNewValue();
               if (newValue != null) {
                  if (oldValue != null) {
                     // Only allow change to be applied if value has not
                     // changed since the start of the processing.
                     cache.replace(key, oldValue, newValue);
                  } else {
                     cache.putIfAbsent(key, newValue);
                  }
               }
            }
            return ret;
         }
      });
   }

   @SuppressWarnings("unchecked")
   private V safeCopy(V original) {
      try {
         StreamingMarshaller marshaller = skipListenerCache.getComponentRegistry().getCacheMarshaller();
         byte[] bytes = marshaller.objectToByteBuffer(original);
         Object o = marshaller.objectFromByteBuffer(bytes);
         return (V) o;
      } catch (Exception e) {
         throw new CacheException(
               "Unexpected error making a copy of entry " + original, e);
      }
   }

   private boolean lockRequired(K key) {
      // Check if processor is locking a key, so that exclusive locking can
      // be avoided for majority of use cases. This way, only when
      // invokeProcessor is locking a key there's a need for CRUD cache
      // methods to acquire the exclusive lock. This latter requirement is
      // specifically tested by the TCK comparing duration of paralell
      // executions.
      boolean locked = processorLocks.isLocked(key);
      if (log.isTraceEnabled())
         log.tracef("Lock required for key=%s? %s", key, locked);

      return locked;
   }

   private void acquiredProcessorLock(K key) throws InterruptedException {
      processorLocks.acquireLock(
            Thread.currentThread(), key, lockTimeout, TimeUnit.MILLISECONDS);
   }

   private void releaseProcessorLock(K key) {
      processorLocks.releaseLock(Thread.currentThread(), key);
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
   public void put(final K key, final V value) {
      checkStarted();
      if (lockRequired(key)) {
         new WithProcessorLock<Void>().call(key, new Callable<Void>() {
            @Override
            public Void call() {
               doPut(key, value);
               return null;
            }
         });
      } else {
         doPut(key, value);
      }
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
      for (final Map.Entry<? extends K, ? extends V> e : inputMap.entrySet()) {
         final K key = e.getKey();
         if (lockRequired(key)) {
            new WithProcessorLock<Void>().call(key, new Callable<Void>() {
               @Override
               public Void call() {
                  doPut(key, e.getValue());
                  return null;
               }
            });
         } else {
            doPut(key, e.getValue());
         }
      }
   }

   private void doPut(K key, V value) {
      // A normal put should not fire notifications when checking TTL
      put(ignoreReturnValuesCache, containsKeyCache, key, value, false);
   }

   @Override
   public boolean putIfAbsent(final K key, final V value) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return put(skipCacheLoadCache,
                     skipCacheLoadCache, key, value, true) == null;
            }
         });
      }

      return put(skipCacheLoadCache,
            skipCacheLoadCache, key, value, true) == null;
   }

   @Override
   public boolean registerCacheEntryListener(
            CacheEntryListener<? super K, ? super V> cacheEntryListener, boolean requireOldValue,
            CacheEntryEventFilter<? super K, ? super V> cacheEntryFilter, boolean synchronous) {
      if (cacheEntryListener == null)
         throw new CacheEntryListenerException("A listener may not be null");

      return notifier.addListenerIfAbsent(new JCacheListenerRegistration<K, V>(
            cacheEntryListener, cacheEntryFilter, requireOldValue, synchronous));
   }

   @Override
   public boolean remove(final K key) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return cache.remove(key) != null;
            }
         });
      }

      return cache.remove(key) != null;
   }

   @Override
   public boolean remove(final K key, final V oldValue) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return cache.remove(key, oldValue);
            }
         });
      }

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
      for (Cache.Entry<K, V> entry : this) {
         final K key = entry.getKey();
         if (lockRequired(key)) {
            new WithProcessorLock<Void>().call(key, new Callable<Void>() {
               @Override
               public Void call() {
                  cache.remove(key);
                  return null;
               }
            });
         } else {
            futures.add(cache.removeAsync(key));
         }
      }

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
   public boolean replace(final K key, final V value) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return replace(skipCacheLoadCache, key, null, value, false);
            }
         });
      }

      return replace(skipCacheLoadCache, key, null, value, false);
   }

   @Override
   public boolean replace(final K key, final V oldValue, final V newValue) {
      checkStarted();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return replace(skipCacheLoadCache, key, oldValue, newValue, true);
            }
         });
      }

      return replace(skipCacheLoadCache, key, oldValue, newValue, true);
   }

   @Override
   public boolean unregisterCacheEntryListener(
         CacheEntryListener<?, ?> cacheEntryListener) {
      return cacheEntryListener != null
            && notifier.removeListener(cacheEntryListener);
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

   private V put(AdvancedCache<K, V> cache, AdvancedCache<K, V> createCheckCache,
         K key, V value, boolean isPutIfAbsent) {
      boolean isCreated;
      // Use a separate cache reference to check whether entry is created or
      // not. A separate reference allows for listener notifications to be
      // skipped selectively.
      isCreated = !createCheckCache.containsKey(key);

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

   private V replace(AdvancedCache<K, V> cache, K key, V value) {
      boolean exists = cache.containsKey(key);
      if (exists) {
         Entry<K, V> entry = new JCacheEntry<K, V>(key, value);
         // TODO: Retrieve existing lifespan setting for entry from internal container?
         Configuration.Duration ttl = expiryPolicy
               .getTTLForModifiedEntry(entry, null);

         if (ttl == null || ttl.isEternal()) {
            return cache.replace(key, value);
         } else if (ttl.equals(Configuration.Duration.ZERO)) {
            // TODO: Can this be avoided?
            // Remove explicitly
            return cache.remove(key);
         } else {
            long duration = ttl.getDurationAmount();
            TimeUnit timeUnit = ttl.getTimeUnit();
            return cache.replace(key, value, duration, timeUnit);
         }
      }

      verifyNewValue(value);
      return null;
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

   private class WithProcessorLock<V> {
      public V call(K key, Callable<V> callable) {
         try {
            acquiredProcessorLock(key);
            return callable.call();
         } catch (InterruptedException e) {
            // restore interrupted status
            Thread.currentThread().interrupt();
            return null;
         } catch (Exception e) {
            throw new CacheException(e);
         } finally {
            releaseProcessorLock(key);
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

         // Return cannot be null here, use cache.get to force
         // next() operation to fire a listener visit event.
         cache.get(ret.getKey());

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
      }
   }

}
