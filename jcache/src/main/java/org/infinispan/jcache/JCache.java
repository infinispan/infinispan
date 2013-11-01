package org.infinispan.jcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.management.MBeanServer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.api.AsyncCache;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.concurrent.FutureListener;
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

   private final ExpiryPolicy expiryPolicy;
   private final LockContainer processorLocks = new ReentrantPerEntryLockContainer(32);
   private final long lockTimeout; // milliseconds
   private final JCacheNotifier<K, V> notifier = new JCacheNotifier<K, V>();
   private CacheLoader<K, V> jcacheLoader;
   private CacheWriter<? super K, ? super V> jcacheWriter;

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

      for (CacheEntryListenerConfiguration<K, V> r
            : c.getCacheEntryListenerConfigurations())
         notifier.addListener(r, this, notifier, cache);

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
         JCacheLoaderAdapter<K, V> adapter = getCacheLoaderAdapter(persistenceManager);
         jcacheLoader = cacheLoaderFactory.create();
         adapter.setCacheLoader(jcacheLoader);
         adapter.setExpiryPolicy(expiryPolicy);
      }
   }

   @SuppressWarnings("unchecked")
   private JCacheLoaderAdapter<K, V> getCacheLoaderAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheLoaderAdapter<K, V>) persistenceManager.getAllLoaders().get(0);
   }

   private void setCacheWriter(AdvancedCache<K, V> cache, Configuration<K, V> c) {
      // Plug user-defined cache writer into adaptor
      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
      if (cacheWriterFactory != null) {
         PersistenceManagerImpl persistenceManager =
               (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
         JCacheWriterAdapter<K, V> ispnCacheStore = getCacheWriterAdapter(persistenceManager);
         jcacheWriter = cacheWriterFactory.create();
         ispnCacheStore.setCacheWriter(jcacheWriter);
      }
   }

   @SuppressWarnings("unchecked")
   private JCacheWriterAdapter<K, V> getCacheWriterAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheWriterAdapter<K, V>) persistenceManager.getAllWriters().get(0);
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
      V value = configuration.isReadThrough() ? cache.get(key) : skipCacheLoadCache.get(key);
      if (value != null)
         updateTTLForAccessed(cache, key, value);

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
      verifyEntryProcessor(entryProcessor);

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

            MutableJCacheEntry<K, V> mutable = createMutableCacheEntry(safeOldValue, key);
            T ret = processEntryProcessor(mutable, entryProcessor, arguments);

            switch (mutable.getOperation()) {
               case NONE:
                  break;
               case ACCESS:
                  skipCacheLoadCache.get(key); // update hit stats
                  updateTTLForAccessed(cache, key, oldValue);
                  break;
               case UPDATE:
                  V newValue = mutable.getNewValue();
                  if (oldValue != null) {
                     // Only allow change to be applied if value has not
                     // changed since the start of the processing.
                     replace(cache, key, oldValue, newValue, true);
                  } else {
                     put(cache, skipCacheLoadCache, key, newValue, true);
                  }
                  break;
               case REMOVE:
                  cache.remove(key);
                  break;
            }

            return ret;
         }
      });
   }

   private <T> T processEntryProcessor(MutableJCacheEntry<K, V> mutable,
         EntryProcessor<K, V, T> entryProcessor, Object[] arguments) {
      try {
         return entryProcessor.process(mutable, arguments);
      } catch (Exception e) {
         throw Exceptions.launderEntryProcessorException(e);
      }
   }

   private MutableJCacheEntry<K, V> createMutableCacheEntry(V safeOldValue, K key) {
      return new MutableJCacheEntry<K, V>(
            configuration.isReadThrough() ? cache : skipCacheLoadCache, key, safeOldValue);
   }

   @Override
   public <T> Map<K, T> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
      checkNotClosed();

      // spec required null checks
      verifyKeys(keys);
      verifyEntryProcessor(entryProcessor);

      Map<K, T> result = new HashMap<K, T>(keys.size());
      for (K key : keys) {
         T t = invoke(key, entryProcessor, arguments);
         if (t != null)
            result.put(key, t);
      }

      return result;
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
   public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      checkNotClosed();

      if (keys == null)
         throw log.parameterMustNotBeNull("keys");

      // Separate logic based on whether a JCache cache loader or an Infinispan
      // cache loader might be plugged. TCK has some strict expectations when
      // it comes to expiry policy usage when loaded entries are stored in the
      // cache for the first time, or if they're overriding a value (forced by
      // replaceExistingValues). These two cases cannot be discerned at the
      // cache loader level. The only alternative way would be for a jcache
      // loader interceptor to be created, but getting the interaction right
      // with existing cache loader interceptor would not be trivial. Hence,
      // we separate logic at this level.

      if (jcacheLoader == null && jcacheWriter != null)
         listener.onCompletion(); // A cache writer cannot load data
      else if (jcacheLoader != null)
         loadAllFromJCacheLoader(keys, replaceExistingValues, listener);
      else
         loadAllFromInfinispanCacheLoader(keys, replaceExistingValues, listener);
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

      try {
         return cache.remove(key) != null;
      } catch (CacheListenerException e) {
         throw Exceptions.launderCacheListenerException(e);
      }
   }

   @Override
   public boolean remove(final K key, final V oldValue) {
      checkNotClosed();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return remove(cache, key, oldValue);
            }
         });
      }

      return remove(cache, key, oldValue);
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
            throw Exceptions.launderCacheWriterException(e);
         }
      }
   }

   @Override
   public void removeAll(Set<? extends K> keys) {
      checkNotClosed();
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

   @Override
   public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.addListener(listenerCfg, this, notifier, cache);
      addCacheEntryListenerConfiguration(listenerCfg);
   }

   @Override
   public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.removeListener(listenerCfg, cache);
      removeCacheEntryListenerConfiguration(listenerCfg);
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

   private <T> void verifyEntryProcessor(EntryProcessor<K, V, T> entryProcessor) {
      if (entryProcessor == null)
         throw new NullPointerException("Entry processor cannot be null");
   }

   private V put(AdvancedCache<K, V> cache, AdvancedCache<K, V> createCheckCache,
         K key, V value, boolean isPutIfAbsent) {
      // Use a separate cache reference to check whether entry is created or
      // not. A separate reference allows for listener notifications to be
      // skipped selectively.
      V prev = createCheckCache.get(key);
      boolean isCreated = prev == null;

      // If putIfAbsent and entry already present, skip early
      if (!isCreated && isPutIfAbsent)
         return prev;

      V ret;
      Duration ttl = isCreated
            ? Expiration.getExpiry(expiryPolicy, Expiration.Operation.CREATION)
            : Expiration.getExpiry(expiryPolicy, Expiration.Operation.UPDATE);

      try {
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
      } catch (CacheListenerException e) {
         throw Exceptions.launderCacheListenerException(e);
      }
   }

   private boolean replace(AdvancedCache<K, V> cache,
         K key, V oldValue, V value, boolean isConditional) {
      V current = cache.get(key);
      if (current != null) {
         if (isConditional && !current.equals(oldValue)) {
            updateTTLForAccessed(cache, key, value);
            return false;
         }

         Duration ttl = Expiration.getExpiry(expiryPolicy, Expiration.Operation.UPDATE);

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
         Duration ttl = Expiration.getExpiry(expiryPolicy, Expiration.Operation.UPDATE);

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

   private void updateTTLForAccessed(AdvancedCache<K, V> cache, K key, V value) {
      Duration ttl = Expiration.getExpiry(expiryPolicy, Expiration.Operation.ACCESS);

      if (ttl != null) {
         if (ttl.equals(Duration.ZERO)) {
            // TODO: Expiry of 0 does not seem to remove entry when next accessed.
            // Hence, explicitly removing the entry.
            cache.remove(key);
         } else {
            // The expiration policy could potentially return different values
            // every time, so don't think we can rely on maxIdle.
            long durationAmount = ttl.getDurationAmount();
            TimeUnit timeUnit = ttl.getTimeUnit();
            cache.put(key, value, durationAmount, timeUnit);
         }
      }
   }

   private void addCacheEntryListenerConfiguration(
         CacheEntryListenerConfiguration<K, V> listenerCfg) {
      if (listenerCfg == null) {
         throw new NullPointerException("CacheEntryListenerConfiguration can't be null");
      }

      boolean alreadyExists = false;
      for (CacheEntryListenerConfiguration<? super K, ? super V> c
            : configuration.getCacheEntryListenerConfigurations()) {
         if (c.equals(listenerCfg)) {
            alreadyExists = true;
         }
      }

      if (!alreadyExists) {
         configuration.getCacheEntryListenerConfigurations().add(listenerCfg);
      } else {
         throw new IllegalArgumentException("A CacheEntryListenerConfiguration can " +
               "be registered only once");
      }
   }

   private void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      configuration.getCacheEntryListenerConfigurations().remove(listenerCfg);
   }

   private void loadAllFromInfinispanCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      final List<K> keysToLoad = filterLoadAllKeys(keys, replaceExistingValues, true);
      if (keysToLoad.isEmpty()) {
         listener.onCompletion();
         return;
      }

      try {
         // Using a cyclic barrier, initialised with the number of keys to load,
         // in order to load all keys asynchronously and when the last one completes,
         // callback to the CompletionListener (via a barrier action).
         final CyclicBarrier barrier = new CyclicBarrier(keysToLoad.size(), new Runnable() {
            @Override
            public void run() {
               if (log.isTraceEnabled())
                  log.tracef("Keys %s loaded, notify listener on completion", keysToLoad);

               listener.onCompletion();
            }
         });
         FutureListener<V> futureListener = new FutureListener<V>() {
            @Override
            public void futureDone(Future<V> future) {
               try {
                  if (log.isTraceEnabled())
                     log.tracef("Key loaded, wait for the rest of keys to load");

                  barrier.await(30, TimeUnit.SECONDS);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } catch (BrokenBarrierException e) {
                  setListenerException(listener, e);
               } catch (TimeoutException e) {
                  setListenerException(listener, e);
               }
            }
         };
         AsyncCache<K, V> asyncCache = cache;
         for (K k : keysToLoad)
            asyncCache.getAsync(k).attachListener(futureListener);

      } catch (Throwable t) {
         log.errorLoadingAll(keysToLoad, t);
         setListenerException(listener, t);
      }
   }

   private List<K> filterLoadAllKeys(Set<? extends K> keys, boolean replaceExistingValues, boolean cacheEvict) {
      if (log.isTraceEnabled())
         log.tracef("Before filtering, keys to load: %s", keys);

      // Filter null keys out and keys to be overriden optionally
      final List<K> keysToLoad = new ArrayList<K>();
      for (K key : keys) {
         if (key == null)
            throw log.parameterMustNotBeNull("Key");

         // Evict from memory if value needs to be replaced (instead of flag)
         if (cacheEvict && replaceExistingValues && containsKey(key))
            cache.evict(key);

         if (replaceExistingValues || !containsKey(key))
            keysToLoad.add(key);
      }

      if (log.isTraceEnabled())
         log.tracef("After filtering, keys to load: %s", keysToLoad);

      return keysToLoad;
   }

   private void loadAllFromJCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      final List<K> keysToLoad = filterLoadAllKeys(keys, replaceExistingValues, false);
      if (keysToLoad.isEmpty()) {
         listener.onCompletion();
         return;
      }

      try {
         Map<K, V> loaded = loadAllKeys(keysToLoad);
         Iterator<Map.Entry<K, V>> it = loaded.entrySet().iterator();
         while (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            if (entry.getValue() == null)
               it.remove();
         }

         // Check and update only in-memory cache. Skip cache load in order to
         // check only in-memory contents. For storing data, use a skip cache
         // store cache in order to avoid storing data in the cache store that
         // has just been loaded.
         AdvancedCache<K, V> skipCacheStoreCache = ignoreReturnValuesCache.withFlags(Flag.SKIP_CACHE_STORE);
         for (Map.Entry<K, V> entry : loaded.entrySet()) {
            K loadedKey = entry.getKey();
            V loadedValue = entry.getValue();
            put(skipCacheStoreCache, skipCacheLoadCache, loadedKey, loadedValue, false);
         }
         listener.onCompletion();
      } catch (Throwable t) {
         setListenerException(listener, t);
      }
   }

   private Map<K, V> loadAllKeys(List<K> keysToLoad) {
      try {
         return jcacheLoader.loadAll(keysToLoad);
      } catch (Exception e) {
         throw Exceptions.launderCacheLoaderException(e);
      }
   }

   private boolean remove(AdvancedCache<K, V> cache, K key, V oldValue) {
      V current = cache.get(key);
      if (current != null && !current.equals(oldValue)) {
         updateTTLForAccessed(cache, key, current);
         return false;
      }

      return cache.remove(key, oldValue);
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
         } catch (CacheListenerException e) {
            throw Exceptions.launderCacheListenerException(e);
         } catch (EntryProcessorException e) {
            throw e;
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
      }
   }

}
