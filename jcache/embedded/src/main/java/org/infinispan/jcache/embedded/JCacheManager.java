package org.infinispan.jcache.embedded;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.Set;

import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheManager;
import org.infinispan.jcache.embedded.logging.Log;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of {@link javax.cache.CacheManager}.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheManager extends AbstractJCacheManager {
   private static final Log log = LogFactory.getLog(JCacheManager.class, Log.class);

   private final EmbeddedCacheManager cm;

   /**
    * Create a new InfinispanCacheManager given a cache name and a {@link ClassLoader}. Cache name
    * might refer to a file on classpath containing Infinispan configuration file.
    *
    * @param uri identifies the cache manager
    * @param classLoader used to load classes stored in this cache manager
    */
   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties properties) {
      super(uri, classLoader, provider, properties, false);

      if (classLoader == null) {
         throw new IllegalArgumentException("Classloader cannot be null");
      }
      if (uri == null) {
         throw new IllegalArgumentException("Invalid CacheManager URI " + uri);
      }

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
   }

   public JCacheManager(URI uri, EmbeddedCacheManager cacheManager, CachingProvider provider) {
      super(uri, null, provider, null, true);
      this.cm = cacheManager;
      registerPredefinedCaches();
   }

   private void registerPredefinedCaches() {
      // TODO get predefined caches and register them
      // TODO galderz find a better way to do this as spec allows predefined caches to be
      // loaded (config file), instantiated and registered with CacheManager
      Set<String> cacheNames = cm.getCacheNames();
      for (String cacheName : cacheNames) {
         // With pre-defined caches, obey only pre-defined configuration
         registerPredefinedCache(cacheName, new JCache<Object, Object>(
               cm.getCache(cacheName).getAdvancedCache(), this,
               ConfigurationAdapter.create()));
      }
   }

   private ConfigurationBuilderHolder getConfigurationBuilderHolder(
         ClassLoader classLoader) {
      try {
         FileLookup fileLookup = FileLookupFactory.newInstance();
         InputStream configurationStream = getURI().isAbsolute()
               ? fileLookup.lookupFileStrict(getURI(), classLoader)
               : fileLookup.lookupFileStrict(getURI().toString(), classLoader);
         return new ParserRegistry(classLoader).parse(configurationStream);
      } catch (FileNotFoundException e) {
         // No such file, lets use default CBH
         return new ConfigurationBuilderHolder(classLoader);
      }
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrapAny(clazz, this, cm);
   }

   @Override
   public ClassLoader getClassLoader() {
      return cm.getCacheManagerConfiguration().classLoader();
   }

   @Override
   protected void delegateLogIsClosed() {
      throw log.cacheManagerClosed(cm.getStatus());
   }

   @Override
   protected void delegateStop() {
      cm.stop();
   }

   @Override
   protected Iterable<String> delegateCacheNames() {
      return cm.getCacheNames();
   }

   @Override
   protected boolean delegateIsClosed() {
      return cm.getStatus().isTerminated();
   }

   @Override
   protected <K, V> void delegateRemoveCache(AbstractJCache<K, V> jcache) {
      cm.removeCache(jcache.getName());
   }

   @Override
   protected <K, V, C extends Configuration<K, V>> AbstractJCache<K, V> create(String cacheName, C configuration) {
      ConfigurationAdapter<K, V> adapter = ConfigurationAdapter.create(configuration);
      cm.defineConfiguration(cacheName, adapter.build());
      AdvancedCache<K, V> ispnCache =
            cm.<K, V>getCache(cacheName).getAdvancedCache();

      // In case the cache was stopped
      if (!ispnCache.getStatus().allowInvocations())
         ispnCache.start();

      return new JCache<K, V>(ispnCache, this, adapter);
   }

   @Override
   protected <K, V, I extends BasicCache<K, V>> AbstractJCache<K, V> create(I ispnCache) {
      return new JCache<K, V>((AdvancedCache<K, V>) ispnCache, this, ConfigurationAdapter.<K, V>create());
   }
}
