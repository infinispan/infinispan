package org.infinispan.jcache;

import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.STATISTICS;
import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.CONFIGURATION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.management.MBeanServer;

import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jcache.logging.Log;

public abstract class AbstractJCache<K, V> implements Cache<K, V> {
   private static final Log log =
         LogFactory.getLog(AbstractJCache.class, Log.class);

   protected final MutableConfiguration<K, V> configuration;
   protected final ExpiryPolicy expiryPolicy;
   protected final AbstractJCacheNotifier<K, V> notifier;

   private final CacheManager cacheManager;

   protected CacheLoader<K, V> jcacheLoader;
   protected CacheWriter<? super K, ? super V> jcacheWriter;

   private final CacheMXBean mxBean;

   public AbstractJCache(MutableConfiguration<K, V> configuration, CacheManager cacheManager, AbstractJCacheNotifier<K, V> notifier) {
      this.configuration = configuration;
      this.cacheManager = cacheManager;
      this.notifier = notifier;
      this.expiryPolicy = configuration.getExpiryPolicyFactory().create();

      this.mxBean = new RIDelegatingCacheMXBean<K, V>(this);
   }

   protected void addConfigurationListeners() {
      for (CacheEntryListenerConfiguration<K, V> r
            : configuration.getCacheEntryListenerConfigurations())
         notifier.addListener(r, this, notifier);
   }

   @Override
   public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
      if (clazz.isInstance(configuration))
         return clazz.cast(configuration);
      throw log.configurationClassNotSupported(clazz);
   }

   public AbstractJCache<K, V> checkNotNull(Object obj, String name) {
      if (obj == null) {
         throw log.parameterMustNotBeNull(name);
      }
      return this;
   }

   protected void setCacheLoader(CompleteConfiguration<K, V> c) {
      // Plug user-defined cache loader into adaptor
      Factory<CacheLoader<K, V>> cacheLoaderFactory = c.getCacheLoaderFactory();
      if (cacheLoaderFactory != null) {
         jcacheLoader = cacheLoaderFactory.create();
         addCacheLoaderAdapter(jcacheLoader);
      }
   }

   protected void setCacheWriter(CompleteConfiguration<K, V> c) {
      // Plug user-defined cache writer into adaptor
      Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory = c.getCacheWriterFactory();
      if (cacheWriterFactory != null) {
         jcacheWriter = cacheWriterFactory.create();
         addCacheWriterAdapter(jcacheWriter);
      }
   }

   protected abstract void addCacheLoaderAdapter(CacheLoader<K, V> cacheLoader);
   protected abstract void addCacheWriterAdapter(CacheWriter<? super K, ? super V> cacheWriter);

   protected void setListenerCompletion(CompletionListener listener) {
      if (listener != null)
         listener.onCompletion();
   }

   protected void setListenerException(CompletionListener listener, Throwable t) {
      if (listener !=  null) {
         if (t instanceof Exception)
            listener.onException((Exception) t);
         else
            listener.onException(new CacheException(t));
      }
   }

   protected List<K> filterLoadAllKeys(Set<? extends K> keys, boolean replaceExistingValues, boolean cacheEvict) {
      if (log.isTraceEnabled())
         log.tracef("Before filtering, keys to load: %s", keys);

      // Filter null keys out and keys to be overridden optionally
      final List<K> keysToLoad = new ArrayList<K>();
      for (K key : keys) {
         if (key == null)
            throw log.parameterMustNotBeNull("Key");

         // Evict from memory if value needs to be replaced (instead of flag)
         if (cacheEvict && replaceExistingValues && containsKey(key))
            evict(key);

         if (replaceExistingValues || !containsKey(key))
            keysToLoad.add(key);
      }

      if (log.isTraceEnabled())
         log.tracef("After filtering, keys to load: %s", keysToLoad);

      return keysToLoad;
   }

   protected Map<K, V> loadAllKeys(List<K> keysToLoad) {
      try {
         return jcacheLoader.loadAll(keysToLoad);
      } catch (Exception e) {
         throw Exceptions.launderCacheLoaderException(e);
      }
   }

   protected void loadAllFromJCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener,
         BasicCache<K, V> cache, BasicCache<K, V> createCheckCache) {
      final List<K> keysToLoad = filterLoadAllKeys(keys, replaceExistingValues, false);
      if (keysToLoad.isEmpty()) {
         setListenerCompletion(listener);
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

         for (Map.Entry<K, V> entry : loaded.entrySet()) {
            K loadedKey = entry.getKey();
            V loadedValue = entry.getValue();
            put(cache, createCheckCache, loadedKey, loadedValue, false);
         }
         setListenerCompletion(listener);
      } catch (Throwable t) {
         setListenerException(listener, t);
      }
   }

   protected <T> T processEntryProcessor(MutableJCacheEntry<K, V> mutable,
         EntryProcessor<K, V, T> entryProcessor, Object[] arguments) {
      try {
         return entryProcessor.process(mutable, arguments);
      } catch (Exception e) {
         throw Exceptions.launderEntryProcessorException(e);
      }
   }

   protected abstract AbstractJCache<K, V> checkNotClosed();

   protected AbstractJCache<K, V> verifyKeys(Set<? extends K> keys) {
      // spec required
      if (keys == null || keys.contains(null))
         throw new NullPointerException("keys is null or keys contains a null: " + keys);

      return this;
   }

   @Override
   public <T> Map<K, EntryProcessorResult<T>> invokeAll(
         Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
      checkNotClosed().checkNotNull(entryProcessor, "entryProcessor").verifyKeys(keys);

      Map<K, EntryProcessorResult<T>> map = new HashMap<K, EntryProcessorResult<T>>(keys.size());
      for (K key : keys) {
         EntryProcessorResult<T> result;
         try {
            T t = invoke(key, entryProcessor, arguments);
            result = t == null ? null : new SuccessEntryProcessorResult<T>(t);
         } catch (Throwable t) {
            result = new FailureEntryProcessorResult<T>(t);
         }
         if (result != null)
            map.put(key, result);
      }

      return map;
   }

   protected abstract void evict(K key);

   protected V put(BasicCache<K, V> cache, BasicCache<K, V> createCheckCache,
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

   protected void updateTTLForAccessed(BasicCache<K, V> cache, K key, V value) {
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

   protected boolean replace(BasicCache<K, V> cache, BasicCache<K, V> existsCheckCache,
         K key, V oldValue, V value, boolean isConditional) {
      checkNotNull(value, "value");
      if (isConditional) {
         // Even if replace fails, values have to be validated (required by TCK)
         checkNotNull(oldValue, "oldValue");
      }

      V current = existsCheckCache.get(key);
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

      return false;
   }

   protected V replace(BasicCache<K, V> cache, K key, V value) {
      checkNotNull(value, "value");
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

      return null;
   }

   protected boolean remove(BasicCache<K, V> cache, K key, V oldValue) {
      V current = cache.get(key);
      if (current != null && !current.equals(oldValue)) {
         updateTTLForAccessed(cache, key, current);
         return false;
      }

      return cache.remove(key, oldValue);
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }

   //TODO: these were initially package-level
   protected abstract MBeanServer getMBeanServer();

   //TODO: these were initially package-level
   protected Object getCacheMXBean() {
      return mxBean;
   }

   //TODO: these were initially package-level
   protected abstract Object getCacheStatisticsMXBean();

   //TODO: was package-level initially
   public void setManagementEnabled(boolean enabled) {
      configuration.setManagementEnabled(enabled);
      if (enabled)
         RIMBeanServerRegistrationUtility.registerCacheObject(this, CONFIGURATION);
      else
         RIMBeanServerRegistrationUtility.unregisterCacheObject(this, CONFIGURATION);
   }

   //TODO: was package-level initially
   public void setStatisticsEnabled(boolean enabled) {
      configuration.setStatisticsEnabled(enabled);
      if (enabled)
         RIMBeanServerRegistrationUtility.registerCacheObject(this, STATISTICS);
      else
         RIMBeanServerRegistrationUtility.unregisterCacheObject(this, STATISTICS);
   }

   protected abstract void addListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter);

   protected abstract void removeListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter);

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   protected void addCacheEntryListenerConfiguration(
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
         configuration.addCacheEntryListenerConfiguration(listenerCfg);
      } else {
         throw new IllegalArgumentException("A CacheEntryListenerConfiguration can " +
               "be registered only once");
      }
   }

   protected void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      configuration.removeCacheEntryListenerConfiguration(listenerCfg);
   }

   protected boolean statisticsEnabled() {
      return getConfiguration(CompleteConfiguration.class).isStatisticsEnabled();
   }
}
