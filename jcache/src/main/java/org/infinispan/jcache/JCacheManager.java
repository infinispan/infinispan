package org.infinispan.jcache;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import javax.cache.*;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jcache.logging.Log;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.CONFIGURATION;
import static org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType.STATISTICS;

/**
 * Infinispan's implementation of {@link javax.cache.CacheManager}.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheManager implements CacheManager {

   private static final Log log =
         LogFactory.getLog(JCacheManager.class, Log.class);

   private final HashMap<String, JCache<?, ?>> caches = new HashMap<String, JCache<?, ?>>();
   private final URI uri;
   private final EmbeddedCacheManager cm;
   private final CachingProvider provider;
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

   /**
    * Create a new InfinispanCacheManager given a cache name and a {@link ClassLoader}. Cache name
    * might refer to a file on classpath containing Infinispan configuration file.
    *
    * @param uri identifies the cache manager
    * @param classLoader used to load classes stored in this cache manager
    */
   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider) {
      // Track allocation time
      this.allocationStackTrace = Thread.currentThread().getStackTrace();

      if (classLoader == null) {
         throw new IllegalArgumentException("Classloader cannot be null");
      }
      if (uri == null) {
         throw new IllegalArgumentException("Invalid CacheManager URI " + uri);
      }

      this.uri = uri;
      this.provider = provider;

      ConfigurationBuilderHolder cbh = getConfigurationBuilderHolder(classLoader);
      GlobalConfigurationBuilder globalBuilder = cbh.getGlobalConfigurationBuilder();
      // The cache manager name has to contain all uri, class loader and
      // provider information in order to guarantee JMX naming uniqueness.
      // This is tested by the TCK to make sure caching provider loaded
      // with different classloaders, even if the default classloader for
      // the cache manager is the same, are really different cache managers.
      String cacheManagerName = "uri=" + uri
            + "/classloader=" + classLoader.toString()
            + "/provider=" + provider.toString();
      // Set cache manager class loader and apply name to cache manager MBean
      globalBuilder.classLoader(classLoader)
            .globalJmxStatistics().cacheManagerName(cacheManagerName);

      cm = new DefaultCacheManager(cbh, true);
      registerPredefinedCaches();

      isClosed = false;
      managedCacheManager = false;
   }

   public JCacheManager(URI uri, EmbeddedCacheManager cacheManager, CachingProvider provider) {
      // Track allocation time
      this.allocationStackTrace = Thread.currentThread().getStackTrace();
      this.uri = uri;
      this.provider = provider;
      this.cm = cacheManager;
      this.managedCacheManager = true;
      registerPredefinedCaches();
   }

   private void registerPredefinedCaches() {
      // TODO get predefined caches and register them
      // TODO galderz find a better way to do this as spec allows predefined caches to be
      // loaded (config file), instantiated and registered with CacheManager
      Set<String> cacheNames = cm.getCacheNames();
      for (String cacheName : cacheNames) {
         // With pre-defined caches, obey only pre-defined configuration
         caches.put(cacheName, new JCache<Object, Object>(
               cm.getCache(cacheName).getAdvancedCache(),
               this, new MutableConfiguration<Object, Object>()));
      }
   }

   private ConfigurationBuilderHolder getConfigurationBuilderHolder(
         ClassLoader classLoader) {
      try {
         FileLookup fileLookup = FileLookupFactory.newInstance();
         InputStream configurationStream = uri.isAbsolute()
               ? fileLookup.lookupFileStrict(uri, classLoader)
               : fileLookup.lookupFileStrict(uri.toString(), classLoader);
         return new ParserRegistry(classLoader).parse(configurationStream);
      } catch (FileNotFoundException e) {
         // No such file, lets use default CBH
         return new ConfigurationBuilderHolder(classLoader);
      }
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
      return null;
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, Configuration<K, V> configuration) {
      checkNotClosed();

      // spec required
      if (cacheName == null)
         throw log.parameterMustNotBeNull("cacheName");

      // spec required
      if (configuration == null)
         throw log.parameterMustNotBeNull("configuration");

      synchronized (caches) {
         JCache<?, ?> cache = caches.get(cacheName);

         if (cache == null) {
            ConfigurationAdapter<K, V> adapter = new ConfigurationAdapter<K, V>(configuration);
            cm.defineConfiguration(cacheName, adapter.build());
            AdvancedCache<K, V> ispnCache =
                  cm.<K, V>getCache(cacheName).getAdvancedCache();

            // In case the cache was stopped
            if (!ispnCache.getStatus().allowInvocations())
               ispnCache.start();

            cache = new JCache<K, V>(ispnCache, this, configuration);
            caches.put(cache.getName(), cache);
         }
         else {
            // re-register attempt with different configuration
            if (cache.getConfiguration().equals(configuration))
               throw log.cacheAlreadyRegistered(cacheName, cache.getConfiguration(), configuration);
         }

         return unchecked(cache);
      }
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
      if (keyType == null)
         throw new NullPointerException("keyType can not be null");

      if (valueType == null)
         throw new NullPointerException("valueType can not be null");

      synchronized (caches) {
         Cache<K, V> cache = unchecked(caches.get(cacheName));
         if (cache != null) {
            Configuration<?, ?> configuration = cache.getConfiguration();

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

   private <K> boolean verifyType(Class<K> type, Class<?> cfgType) {
      return cfgType != null && cfgType.equals(type);
   }

   public <K, V> Cache<K, V> configureCache(String cacheName, AdvancedCache<K, V> ispnCache) {
      synchronized (caches) {
         JCache<?, ?> cache = caches.get(cacheName);
         if (cache == null) {
            cache = new JCache<K, V>(ispnCache, this, new MutableConfiguration<K, V>());
            caches.put(cacheName, cache);
         }
         return unchecked(cache);
      }
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      checkNotClosed();
      synchronized (caches) {
         Cache<K, V> cache = unchecked(caches.get(cacheName));
         if (cache != null) {
            Configuration<K, V> configuration = cache.getConfiguration();
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
      return isClosed ? InfinispanCollections.<String>emptyList() : cm.getCacheNames();
   }

   @Override
   public void destroyCache(String cacheName) {
      checkNotClosed();
      if (cacheName == null)
         throw new NullPointerException("cacheName cannot be null");

      JCache<?, ?> destroyedCache;
      synchronized (caches) {
         destroyedCache = caches.remove(cacheName);
      }

      cm.removeCache(cacheName);
      unregisterCacheMBeans(destroyedCache);
   }

   private void unregisterCacheMBeans(JCache<?, ?> cache) {
      if (cache != null) {
         RIMBeanServerRegistrationUtility.unregisterCacheObject(cache, STATISTICS);
         RIMBeanServerRegistrationUtility.unregisterCacheObject(cache, CONFIGURATION);
      }
   }

   @Override
   public void enableManagement(String cacheName, boolean enabled) {
      caches.get(cacheName).setManagementEnabled(enabled);
   }

   @Override
   public void enableStatistics(String cacheName, boolean enabled) {
      caches.get(cacheName).setStatisticsEnabled(enabled);
   }

   @Override
   public void close() {
      if (!isClosed()) {
         ArrayList<JCache<?, ?>> cacheList;
         synchronized (caches) {
            cacheList = new ArrayList<JCache<?, ?>>(caches.values());
            caches.clear();
         }
         for (JCache<?, ?> cache : cacheList) {
            try {
               cache.close();
               unregisterCacheMBeans(cache);
            } catch (Exception e) {
               // log?
            }
         }
         cm.stop();
         isClosed = true;
      }
   }

   @Override
   public boolean isClosed() {
      return cm.getStatus().isTerminated() || isClosed;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
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
            cm.stop();
         }
      } finally {
         super.finalize();
      }
   }

   private void checkNotClosed() {
      if (isClosed())
         throw new IllegalStateException("Cache manager is in " + cm.getStatus() + " status");
   }

   @SuppressWarnings("unchecked")
   private <K, V> Cache<K, V> unchecked(Cache<?, ?> cache) {
      return (Cache<K, V>) cache;
   }

}
