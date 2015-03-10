package org.infinispan.jcache;

import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.CONFIGURATION;
import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.STATISTICS;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.jcache.logging.Log;

/**
 * Infinispan's implementation of {@link javax.cache.CacheManager}.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 */
public abstract class AbstractJCacheManager implements CacheManager {
   private static final Log log = LogFactory.getLog(AbstractJCacheManager.class, Log.class);

   private final URI uri;
   private final CachingProvider provider;
   private final Properties properties;

   private final HashMap<String, AbstractJCache<?, ?>> caches = new HashMap<String, AbstractJCache<?, ?>>();

   private final StackTraceElement[] allocationStackTrace;

   /**
    * Boolean flag tracking down whether the underlying Infinispan cache
    * manager used by JCacheManager is unmanaged or managed. Unmanaged means
    * that this JCacheManager instance controls the lifecycle of the
    * Infinispan Cache Manager. When managed, it means that the cache manager
    * is injected and hence JCacheManager is not the owner of the lifecycle
    * of this cache manager.
    */
   private final boolean managedCacheManager;

   /**
    * A flag indicating whether the cache manager is closed or not.
    * Cache manager's status does not fit well here because even if an
    * trying to stop a cache manager whose status is {@link ComponentStatus#INSTANTIATED}
    * does not change it to {@link ComponentStatus#TERMINATED}
    */
   private volatile boolean isClosed;

   public AbstractJCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties properties, boolean managedCacheManager) {
      // Track allocation time
      this.allocationStackTrace = Thread.currentThread().getStackTrace();

      this.uri = uri;
      this.provider = provider;
      this.properties = properties;
      this.managedCacheManager = managedCacheManager;
      isClosed = false;
   }

   @Override
   public CachingProvider getCachingProvider() {
      return provider;
   }

   @Override
   public URI getURI() {
      return uri;
   }

   @Override
   public Properties getProperties() {
      return properties;
   }

   @Override
   public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(
         String cacheName, C configuration) {
      checkNotClosed().checkNull(cacheName, "cacheName").checkNull(configuration, "configuration");

      synchronized (caches) {
         AbstractJCache<?, ?> cache = caches.get(cacheName);

         if (cache == null) {
            cache = create(cacheName, configuration);
            caches.put(cache.getName(), cache);
         } else {
            throw log.cacheAlreadyRegistered(cacheName,
                  cache.getConfiguration(Configuration.class), configuration);
         }

         return unchecked(cache);
      }
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
      checkNotClosed().checkNull(keyType, "keyType").checkNull(valueType, "valueType");

      synchronized (caches) {
         Cache<K, V> cache = unchecked(caches.get(cacheName));
         if (cache != null) {
            Configuration<?, ?> configuration = cache.getConfiguration(Configuration.class);

            Class<?> cfgKeyType = configuration.getKeyType();
            if (verifyType(keyType, cfgKeyType)) {
               Class<?> cfgValueType = configuration.getValueType();
               if (verifyType(valueType, cfgValueType))
                  return cache;

               throw log.incompatibleType(valueType, cfgValueType);
            }

            throw log.incompatibleType(keyType, cfgKeyType);
         }

         return null;
      }
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      checkNotClosed();
      synchronized (caches) {
         Cache<K, V> cache = unchecked(caches.get(cacheName));
         if (cache != null) {
            Configuration<K, V> configuration = cache.getConfiguration(Configuration.class);
            Class<K> keyType = configuration.getKeyType();
            Class<V> valueType = configuration.getValueType();
            if (Object.class.equals(keyType) && Object.class.equals(valueType))
               return cache;

            throw log.unsafeTypedCacheRequest(cacheName, keyType, valueType);
         }

         return null;
      }
   }

   @Override
   public Iterable<String> getCacheNames() {
      return isClosed ? InfinispanCollections.<String>emptyList() : delegateCacheNames();
   }

   @Override
   public void destroyCache(String cacheName) {
      checkNotClosed().checkNull(cacheName, "cacheName");

      AbstractJCache<?, ?> destroyedCache;
      synchronized (caches) {
         destroyedCache = caches.remove(cacheName);
      }

      if (destroyedCache != null) {
         /* Don't destroy caches not created through jcache. */
         delegateRemoveCache(destroyedCache);
      }
      unregisterCacheMBeans(destroyedCache);
   }

   @Override
   public void enableManagement(String cacheName, boolean enabled) {
      checkNotClosed();
      caches.get(cacheName).setManagementEnabled(enabled);
   }

   @Override
   public void enableStatistics(String cacheName, boolean enabled) {
      checkNotClosed();
      caches.get(cacheName).setStatisticsEnabled(enabled);
   }

   @Override
   public void close() {
      if (!isClosed()) {
         ArrayList<AbstractJCache<?, ?>> cacheList;
         synchronized (caches) {
            cacheList = new ArrayList<AbstractJCache<?, ?>>(caches.values());
            caches.clear();
         }
         for (AbstractJCache<?, ?> cache : cacheList) {
            try {
               cache.close();
               unregisterCacheMBeans(cache);
            } catch (Exception e) {
               // log?
            }
         }
         delegateStop();
         isClosed = true;
      }
   }

   @Override
   public boolean isClosed() {
      return delegateIsClosed() || isClosed;
   }

   /**
    * Avoid weak references to this cache manager
    * being garbage collected without being shutdown.
    */
   @Override
   protected void finalize() throws Throwable {
      try {
         if(!managedCacheManager && !isClosed) {
            // Create the leak description
            Throwable t = log.cacheManagerNotClosed();
            t.setStackTrace(allocationStackTrace);
            log.leakedCacheManager(t);
            // Close
            delegateStop();
         }
      } finally {
         super.finalize();
      }
   }

   public <K, V, I extends BasicCache<K, V> > Cache<K, V> getOrCreateCache(String cacheName, I ispnCache) {
      synchronized (caches) {
         AbstractJCache<?, ?> cache = caches.get(cacheName);
         if (cache == null) {
            cache = create(ispnCache);
            caches.put(cacheName, cache);
         }
         return unchecked(cache);
      }
   }

   protected <K, V> void registerPredefinedCache(String cacheName, AbstractJCache<K, V> cache) {
      caches.put(cacheName, cache);
   }

   protected abstract void delegateLogIsClosed();
   protected abstract Iterable<String> delegateCacheNames();
   protected abstract void delegateStop();
   protected abstract boolean delegateIsClosed();
   protected abstract <K, V> void delegateRemoveCache(AbstractJCache<K, V> cacheName);

   protected abstract <K, V, C extends Configuration<K, V>> AbstractJCache<K, V> create(String cacheName, C configuration);
   protected abstract <K, V, I extends BasicCache<K, V> > AbstractJCache<K, V> create(I ispnCache);

   protected Set<String> getManagedCacheNames() {
      HashSet<String> result = new HashSet<String>();
      synchronized (caches) {
         result.addAll(caches.keySet());
      }
      return Collections.unmodifiableSet(result);
   }

   private void unregisterCacheMBeans(AbstractJCache<?, ?> cache) {
      if (cache != null) {
         RIMBeanServerRegistrationUtility.unregisterCacheObject(cache, STATISTICS);
         RIMBeanServerRegistrationUtility.unregisterCacheObject(cache, CONFIGURATION);
      }
   }

   private AbstractJCacheManager checkNotClosed() {
      if (isClosed())
         delegateLogIsClosed();

      return this;
   }

   private AbstractJCacheManager checkNull(Object obj, String name) {
      if (obj == null)
         throw log.parameterMustNotBeNull(name);

      return this;
   }

   @SuppressWarnings("unchecked")
   private <K, V> Cache<K, V> unchecked(Cache<?, ?> cache) {
      return (Cache<K, V>) cache;
   }

   private <K> boolean verifyType(Class<K> type, Class<?> cfgType) {
      return cfgType != null && cfgType.equals(type);
   }
}