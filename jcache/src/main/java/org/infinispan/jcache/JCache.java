package org.infinispan.jcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.cache.*;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.management.MBeanServer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.jcache.interceptor.ExpirationTrackingInterceptor;
import org.infinispan.jcache.logging.Log;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.CONFIGURATION;
import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.STATISTICS;

/**
 * Infinispan's implementation of {@link javax.cache.Cache} interface.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class JCache<K, V> implements Cache<K, V> {

   private static final Log log =
         LogFactory.getLog(JCache.class, Log.class);

   private final JCacheManager cacheManager;
   private final Configuration<K, V> configuration;
   private final AdvancedCache<K, V> cache;
   private final AdvancedCache<K, V> ignoreReturnValuesCache;
   private final AdvancedCache<K, V> skipCacheLoadCache;
   private final AdvancedCache<K, V> skipCacheLoadAndStatsCache;
   private final AdvancedCache<K, V> skipListenerCache;
   private final CacheStatisticsMXBean stats;
   private final CacheMXBean mxBean;

   private final ExpiryPolicy<? super K, ? super V> expiryPolicy;
   private final LockContainer processorLocks = new ReentrantPerEntryLockContainer(32);
   private final long lockTimeout; // milliseconds
   private CacheLoader<K, V> cacheLoader;

   public JCache(AdvancedCache<K, V> cache, JCacheManager cacheManager, Configuration<K, V> c) {
      this.cache = cache;
      this.ignoreReturnValuesCache = cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.skipCacheLoadCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);
      this.skipCacheLoadAndStatsCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_STATISTICS);
      // Typical use cases of the SKIP_LISTENER_NOTIFICATION is when trying
      // to comply with specifications such as JSR-107, which mandate that
      // {@link Cache#clear()}} calls do not fire entry removed notifications
      this.skipListenerCache = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
      this.cacheManager = cacheManager;

      // A configuration copy as required by the spec
      // Management enabled setting is not copied in 0.7, this is a workaround
      this.configuration = new MutableConfiguration<K, V>(c)
            .setManagementEnabled(c.isManagementEnabled());

      this.mxBean = new RIDelegatingCacheMXBean<K, V>(this);
      this.stats = new RICacheStatistics(this.cache);
      this.expiryPolicy = configuration.getExpiryPolicyFactory().create();
      this.lockTimeout =  cache.getCacheConfiguration()
            .locking().lockAcquisitionTimeout();

      JCacheNotifier<K, V> notifier = new JCacheNotifier<K, V>();
      boolean hasListeners = false;
      for (CacheEntryListenerConfiguration<K, V> r
            : c.getCacheEntryListenerConfigurations()) {
         notifier.addListener(r);
         hasListeners = true;
      }

      if (hasListeners)
         cache.addListener(new JCacheListenerAdapter<K, V>(this, notifier));

      setCacheLoader(cache, c);
      setCacheWriter(cache, c);
      addExpirationTrackingInterceptor(cache, notifier);

      if (configuration.isManagementEnabled())
         setManagementEnabled(true);

      if (configuration.isStatisticsEnabled())
         setStatisticsEnabled(true);
   }

   private void setCacheLoader(AdvancedCache<K, V> cache, Configuration<K, V> c) {
      // Plug user-defined cache loader into adaptor
      Factory<CacheLoader<K, V>> cacheLoaderFactory = c.getCacheLoaderFactory();
      if (cacheLoaderFactory != null) {
         PersistenceManagerImpl persistenceManager =
               (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
         JCacheLoaderAdapter<K, V> ispnCacheLoader =
               (JCacheLoaderAdapter<K, V>) persistenceManager.getAllLoaders().get(0);
         cacheLoader = cacheLoaderFactory.create();
         ispnCacheLoader.setCacheLoader(cacheLoader);
      }
   }

   private void setCacheWriter(AdvancedCache<K, V> cache, Configuration<K, V> c) {
      // Plug user-defined cache writer into adaptor
      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
      if (cacheWriterFactory != null) {
         PersistenceManagerImpl persistenceManager =
               (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
         JCacheWriterAdapter<K, V> ispnCacheStore =
               (JCacheWriterAdapter) persistenceManager.getAllWriters().get(0);
         ispnCacheStore.setCacheWriter(cacheWriterFactory.create());
      }
   }

   private void addExpirationTrackingInterceptor(AdvancedCache<K, V> cache, JCacheNotifier notifier) {
      ExpirationTrackingInterceptor interceptor = new ExpirationTrackingInterceptor(
            cache.getDataContainer(), this, notifier, cache.getComponentRegistry().getTimeService());
      cache.addInterceptorBefore(interceptor, EntryWrappingInterceptor.class);
   }

   @Override
   public void clear() {
      // TCK expects clear() to not fire any remove events
      skipListenerCache.clear();
   }

   @Override
   public boolean containsKey(final K key) {
      checkNotClosed();

      if (log.isTraceEnabled())
         log.tracef("Invoke containsKey(key=%s)", key);

      if (key == null)
         throw log.parameterMustNotBeNull("key");

      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return skipCacheLoadCache.containsKey(key);
            }
         });
      }

      return skipCacheLoadCache.containsKey(key);
   }

   @Override
   public V get(final K key) {
      checkNotClosed();
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
      checkNotClosed();
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
      checkNotClosed();
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return put(skipCacheLoadCache, skipCacheLoadAndStatsCache, key, value, false);
            }
         });
      }
      return put(skipCacheLoadCache, skipCacheLoadAndStatsCache, key, value, false);
   }

   @Override
   public V getAndRemove(final K key) {
      checkNotClosed();
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
      checkNotClosed();
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
   public void close() {
      cache.stop();
   }

   @Override
   public boolean isClosed() {
      return cache.getStatus().isTerminated();
   }

   @Override
   public Configuration<K, V> getConfiguration() {
      return configuration;
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments) {
      checkNotClosed();
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
            V oldValue = skipCacheLoadAndStatsCache.get(key);
            V safeOldValue = oldValue;
            if (configuration.isStoreByValue()) {
               // Make a copy because the entry processor could make changes
               // directly in the value, and we wanna keep a safe old value
               // around for when calling the atomic replace() call.
               safeOldValue = safeCopy(oldValue);
            }

            MutableJCacheEntry<K, V> mutable =
                  new MutableJCacheEntry<K, V>(cache, key, safeOldValue);
            T ret = entryProcessor.process(mutable, arguments);

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

   @Override
   public <T> Map<K, T> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
      return null;  // TODO: Customise this generated block
   }

   @SuppressWarnings("unchecked")
   private V safeCopy(V original) {
      try {
         StreamingMarshaller marshaller = skipCacheLoadCache.getComponentRegistry().getCacheMarshaller();
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
      return new Itr();
   }

   @Override
   public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener listener) {
      checkNotClosed();

      if (keys == null)
         throw log.parameterMustNotBeNull("keys");

      if (cacheLoader == null) return;

      // Filter null keys out
      List<K> keysToLoad = new ArrayList<K>(keys);
      for (K key : keys) {
         if (key == null)
            throw log.parameterMustNotBeNull("Key");
      }

      try {
         Map<K, V> loaded = cacheLoader.loadAll(keysToLoad);
         Iterator<Map.Entry<K, V>> it = loaded.entrySet().iterator();
         while (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            if (entry.getValue() == null)
               it.remove();
         }

         cache.putAll(loaded);
         listener.onCompletion();
      } catch (Throwable t) {
         setListenerException(listener, t);
      }
   }

   private void setListenerException(CompletionListener listener, Throwable t) {
      if (t instanceof Exception)
         listener.onException((Exception) t);
      else
         listener.onException(new CacheException(t));
   }

   @Override
   public void put(final K key, final V value) {
      checkNotClosed();
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
      checkNotClosed();
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
      put(ignoreReturnValuesCache, skipCacheLoadAndStatsCache, key, value, false);
   }

   @Override
   public boolean putIfAbsent(final K key, final V value) {
      checkNotClosed();
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
   public boolean remove(final K key) {
      checkNotClosed();
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
      checkNotClosed();
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
      checkNotClosed();
      // TODO remove but notify listeners
      verifyKeys(keys);
      for (K k : keys) {
         remove(k);
      }
   }

   @Override
   public boolean replace(final K key, final V value) {
      checkNotClosed();
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
      checkNotClosed();
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
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   void setManagementEnabled(boolean enabled) {
      if (enabled)
         RIMBeanServerRegistrationUtility.registerCacheObject(this, CONFIGURATION);
      else
         RIMBeanServerRegistrationUtility.unregisterCacheObject(this, CONFIGURATION);
   }

   void setStatisticsEnabled(boolean enabled) {
      if (enabled) {
         cache.getStats().setStatisticsEnabled(enabled);
         RIMBeanServerRegistrationUtility.registerCacheObject(this, STATISTICS);
      } else {
         RIMBeanServerRegistrationUtility.unregisterCacheObject(this, STATISTICS);
         cache.getStats().setStatisticsEnabled(enabled);
      }
   }

   CacheMXBean getCacheMXBean() {
      return mxBean;
   }

   CacheStatisticsMXBean getCacheStatisticsMXBean() {
      return stats;
   }

   MBeanServer getMBeanServer() {
      return JmxUtil.lookupMBeanServer(
            cache.getCacheManager().getCacheManagerConfiguration());
   }

   private void checkNotClosed() {
      if (isClosed())
         throw new IllegalStateException("Cache is in " + cache.getStatus() + " state");
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
      Duration ttl;
      Entry<K, V> entry = new JCacheEntry<K, V>(key, value);
      if (isCreated) {
         ttl = expiryPolicy.getExpiryForCreatedEntry(entry);
      } else {
         // TODO: Retrieve existing lifespan setting for entry from internal container?
         ttl = expiryPolicy.getExpiryForModifiedEntry(entry);
      }

      if (ttl == null || ttl.isEternal()) {
         ret = isPutIfAbsent
               ? cache.putIfAbsent(key, value)
               : cache.put(key, value);
      } else if (ttl.equals(Duration.ZERO)) {
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
         Duration ttl = expiryPolicy.getExpiryForModifiedEntry(entry);

         if (ttl == null || ttl.isEternal()) {
            return isConditional
                  ? cache.replace(key, oldValue, value)
                  : cache.replace(key, value) != null;
         } else if (ttl.equals(Duration.ZERO)) {
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
         Duration ttl = expiryPolicy.getExpiryForModifiedEntry(entry);

         if (ttl == null || ttl.isEternal()) {
            return cache.replace(key, value);
         } else if (ttl.equals(Duration.ZERO)) {
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
      Duration ttl = expiryPolicy.getExpiryForAccessedEntry(entry);

      if (ttl != null) {
         if (ttl.equals(Duration.ZERO)) {
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
         } catch (Throwable t) {
            throw new CacheException(t);
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
