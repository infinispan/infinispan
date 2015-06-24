package org.infinispan.jcache.embedded;

import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.CONFIGURATION;
import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.STATISTICS;

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

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
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
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.AbstractJCacheNotifier;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.jcache.MutableJCacheEntry;
import org.infinispan.jcache.RIDelegatingCacheMXBean;
import org.infinispan.jcache.RIMBeanServerRegistrationUtility;
import org.infinispan.jcache.embedded.interceptor.ExpirationTrackingInterceptor;
import org.infinispan.jcache.embedded.logging.Log;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
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
public class JCache<K, V> extends AbstractJCache<K, V> {

   private static final Log log =
         LogFactory.getLog(JCache.class, Log.class);

   private final AdvancedCache<K, V> cache;
   private final AdvancedCache<K, V> ignoreReturnValuesCache;
   private final AdvancedCache<K, V> skipCacheLoadCache;
   private final AdvancedCache<K, V> skipCacheLoadAndStatsCache;
   private final AdvancedCache<K, V> skipListenerCache;
   private final AdvancedCache<K, V> skipStatisticsCache;
   private final RICacheStatistics stats;

   private final LockContainer processorLocks;
   private final long lockTimeout; // milliseconds

   public JCache(AdvancedCache<K, V> cache, CacheManager cacheManager, ConfigurationAdapter<K, V> c) {
      super(c.getConfiguration(), cacheManager, new JCacheNotifier<K, V>());
      this.cache = cache;
      this.processorLocks = new ReentrantPerEntryLockContainer(32, cache.getCacheConfiguration().dataContainer().keyEquivalence());
      this.ignoreReturnValuesCache = cache.withFlags(Flag.IGNORE_RETURN_VALUES);
      this.skipCacheLoadCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);
      this.skipCacheLoadAndStatsCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_STATISTICS);
      // Typical use cases of the SKIP_LISTENER_NOTIFICATION is when trying
      // to comply with specifications such as JSR-107, which mandate that
      // {@link Cache#clear()}} calls do not fire entry removed notifications
      this.skipListenerCache = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
      this.skipStatisticsCache = cache.withFlags(Flag.SKIP_STATISTICS);

      this.stats = new RICacheStatistics(this.cache);
      this.lockTimeout =  cache.getCacheConfiguration()
            .locking().lockAcquisitionTimeout();

      addConfigurationListeners();

      setCacheLoader(configuration);
      setCacheWriter(configuration);
      addExpirationTrackingInterceptor(cache, notifier);

      if (configuration.isManagementEnabled())
         setManagementEnabled(true);

      if (configuration.isStatisticsEnabled())
         setStatisticsEnabled(true);
   }

   protected void addCacheLoaderAdapter(CacheLoader<K, V> cacheLoader) {
      PersistenceManagerImpl persistenceManager =
            (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
      JCacheLoaderAdapter<K, V> adapter = getCacheLoaderAdapter(persistenceManager);
      adapter.setCacheLoader(jcacheLoader);
      adapter.setExpiryPolicy(expiryPolicy);
   }

   @Override
   protected void addCacheWriterAdapter(CacheWriter<? super K, ? super V> cacheWriter) {
      PersistenceManagerImpl persistenceManager =
            (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
      JCacheWriterAdapter<K, V> ispnCacheStore = getCacheWriterAdapter(persistenceManager);
      ispnCacheStore.setCacheWriter(jcacheWriter);
   }

   @SuppressWarnings("unchecked")
   private JCacheLoaderAdapter<K, V> getCacheLoaderAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheLoaderAdapter<K, V>) persistenceManager.getAllLoaders().get(0);
   }

   @SuppressWarnings("unchecked")
   private JCacheWriterAdapter<K, V> getCacheWriterAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheWriterAdapter<K, V>) persistenceManager.getAllWriters().get(0);
   }

   private void addExpirationTrackingInterceptor(AdvancedCache<K, V> cache, AbstractJCacheNotifier notifier) {
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

      return skipCacheLoadAndStatsCache.containsKey(key);
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

      AdvancedCache<K, V> cache = configuration.isReadThrough() ? this.cache : 
         this.skipCacheLoadCache;
      return cache.getAll(keys);
   }

   @Override
   public V getAndPut(final K key, final V value) {
      checkNotClosed();
      if (lockRequired(key)) {
         return new WithProcessorLock<V>().call(key, new Callable<V>() {
            @Override
            public V call() {
               return put(skipCacheLoadCache, skipCacheLoadCache, key, value, false);
            }
         });
      }
      return put(skipCacheLoadCache, skipCacheLoadCache, key, value, false);
   }

   @Override
   public V getAndRemove(final K key) {
      checkNotClosed();
      skipCacheLoadCache.get(key); // bring in key and update stats
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
   public void close() {
      cache.stop();
   }

   @Override
   public boolean isClosed() {
      return cache.getStatus().isTerminated();
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments) {
      checkNotClosed().checkNotNull(key, "key").checkNotNull(entryProcessor, "entryProcessor");

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
            V oldValue = skipCacheLoadCache.get(key);
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
                  updateTTLForAccessed(cache, key, oldValue);
                  break;
               case UPDATE:
                  V newValue = mutable.getNewValue();
                  if (oldValue != null) {
                     // Only allow change to be applied if value has not
                     // changed since the start of the processing.
                     replace(cache, skipCacheLoadAndStatsCache, key, oldValue, newValue, true);
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

   private MutableJCacheEntry<K, V> createMutableCacheEntry(V safeOldValue, K key) {
      return new MutableJCacheEntry<K, V>(
            configuration.isReadThrough() ? cache : skipCacheLoadCache, skipStatisticsCache, key, safeOldValue);
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
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }
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
         setListenerCompletion(listener);
      else if (jcacheLoader != null) {
         loadAllFromJCacheLoader(keys, replaceExistingValues, listener, ignoreReturnValuesCache.withFlags(Flag.SKIP_CACHE_STORE), skipCacheLoadCache);
      } else
         loadAllFromInfinispanCacheLoader(keys, replaceExistingValues, listener);
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
                     skipCacheLoadAndStatsCache, key, value, true) == null;
            }
         });
      }

      return put(skipCacheLoadCache,
            skipCacheLoadAndStatsCache, key, value, true) == null;
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
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }
      // Calling cache.clear() won't work since there's currently no way to
      // for an Infinispan cache store to figure out all keys store and pass
      // them to CacheWriter.deleteAll(), hence, delete individually.
      // TODO: What happens with entries only in store but not in memory?

      // Delete asynchronously and then wait for removals to complete
      List<Future<V>> futures = new ArrayList<Future<V>>();
      for (final K key : cache.keySet()) {
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
               return replace(skipCacheLoadCache, skipCacheLoadCache,
                     key, null, value, false);
            }
         });
      }

      return replace(skipCacheLoadCache, skipCacheLoadCache,
            key, null, value, false);
   }

   @Override
   public boolean replace(final K key, final V oldValue, final V newValue) {
      checkNotClosed();
      if (lockRequired(key)) {
         return new WithProcessorLock<Boolean>().call(key, new Callable<Boolean>() {
            @Override
            public Boolean call() {
               return replace(skipCacheLoadCache, skipCacheLoadCache,
                     key, oldValue, newValue, true);
            }
         });
      }

      return replace(skipCacheLoadCache, skipCacheLoadCache,
            key, oldValue, newValue, true);
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrapAny(clazz, this, cache);
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

   public void setStatisticsEnabled(boolean enabled) {
      cache.getStats().setStatisticsEnabled(enabled);
      super.setStatisticsEnabled(enabled);
   }

   protected CacheStatisticsMXBean getCacheStatisticsMXBean() {
      return stats;
   }

   protected MBeanServer getMBeanServer() {
      return JmxUtil.lookupMBeanServer(
            cache.getCacheManager().getCacheManagerConfiguration());
   }

   protected AbstractJCache<K, V> checkNotClosed() {
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }

      return this;
   }

   private void loadAllFromInfinispanCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      final List<K> keysToLoad = filterLoadAllKeys(keys, replaceExistingValues, true);
      if (keysToLoad.isEmpty()) {
         setListenerCompletion(listener);
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

               setListenerCompletion(listener);
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
         } catch (CacheException e) {
            throw e;
         } catch (Exception e) {
            throw new EntryProcessorException(e);
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
         long start = statisticsEnabled() ? System.nanoTime() : 0;
         if (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            next = new JCacheEntry<K, V>(
                  entry.getKey(), entry.getValue());

            if (statisticsEnabled()) {
               stats.increaseCacheHits(1);
               stats.addGetTimeNano(System.nanoTime() - start);
            }
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

   @Override
   protected void addListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.addListener(listenerAdapter);
   }

   @Override
   protected void removeListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.removeListener(listenerAdapter);
   }

   @Override
   protected void evict(K key) {
      cache.evict(key);
   }
}
