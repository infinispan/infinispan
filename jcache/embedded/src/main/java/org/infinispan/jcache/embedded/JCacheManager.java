package org.infinispan.jcache.embedded;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.dataconversion.MediaType;
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
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of {@link javax.cache.CacheManager}.
 * <p>
 * It is possible to manipulate the configuration programmatically by supplying objects through the <tt>properties</tt>
 * parameter. The following list contains the supported special property names:
 * <ul>
 *    <li><tt>org.infinispan.configuration.parsing.ConfigurationBuilderHolder</tt> an instance of {@link ConfigurationBuilderHolder}
 *    that will be used instead of attempting to load the configuration file specified by the <tt>uri</tt> parameter.</li>
 *    <li><tt>org.infinispan.configuration.global.GlobalConfigurationBuilder</tt> an instance of {@link GlobalConfigurationBuilder}
 *    that will be used as thebase GlobalConfigurationBuilder before parsing the file specified by the <tt>uri</tt> parameter.</li>
 *    <li><tt>org.infinispan.configuration.global.GlobalConfigurationBuilder-Consumer</tt> an instance of {@link Consumer<GlobalConfigurationBuilder>}
 *    that will be used to manipulate the global configuration after parsing the file specified by the <tt>uri</tt> parameter.</li>
 *    <li><tt>org.infinispan.configuration.cache.Configuration-Function</tt> an instance of {@link Function<String, org.infinispan.configuration.cache.Configuration>}
 *    that will be used to obtain the configuration for a cache. If the function returns null, it will use the default configuration.</li>
 * </ul>
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheManager extends AbstractJCacheManager {
   private static final Log log = LogFactory.getLog(JCacheManager.class, Log.class);
   public static final String CACHE_CONFIGURATION_FUNCTION = org.infinispan.configuration.cache.Configuration.class.getName() + "-Function";
   public static final String GLOBAL_CONFIGURATION_CONSUMER = GlobalConfigurationBuilder.class.getName() + "-Consumer";

   private final EmbeddedCacheManager cm;
   private final InternalCacheRegistry icr;

   /**
    * Create a new InfinispanCacheManager given a cache name and a {@link ClassLoader}. Cache name might refer to a file
    * on classpath containing Infinispan configuration file.
    *
    * @param uri         identifies the cache manager
    * @param classLoader used to load classes stored in this cache manager
    * @param properties  properties used to customize the configuration.
    */
   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties properties) {
      super(uri, classLoader, provider, properties, false);

      if (classLoader == null) {
         throw new IllegalArgumentException("Classloader cannot be null");
      }
      if (uri == null) {
         throw new IllegalArgumentException("Invalid CacheManager URI " + uri);
      }

      ConfigurationBuilderHolder cbh;
      if (properties != null && properties.containsKey(ConfigurationBuilderHolder.class.getName())) {
         cbh = (ConfigurationBuilderHolder) properties.get(ConfigurationBuilderHolder.class.getName());
      } else if (properties != null && properties.containsKey(GlobalConfigurationBuilder.class.getName())) {
         cbh = new ConfigurationBuilderHolder(classLoader, (GlobalConfigurationBuilder) properties.get(GlobalConfigurationBuilder.class.getName()));
      } else {
         cbh = new ConfigurationBuilderHolder(classLoader);
      }
      loadConfigurationFromURI(cbh);

      GlobalConfigurationBuilder globalBuilder = cbh.getGlobalConfigurationBuilder();
      if (properties != null && properties.containsKey(GLOBAL_CONFIGURATION_CONSUMER)) {
         Consumer<GlobalConfigurationBuilder> consumer = (Consumer<GlobalConfigurationBuilder>) properties.get(GLOBAL_CONFIGURATION_CONSUMER);
         consumer.accept(globalBuilder);
      }

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
            .cacheManagerName(cacheManagerName)
            .cacheContainer().statistics(true)
            .jmx().enabled(true);

      cm = new DefaultCacheManager(cbh, true);
      icr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(InternalCacheRegistry.class);

      registerPredefinedCaches();
   }

   public JCacheManager(URI uri, EmbeddedCacheManager cacheManager, CachingProvider provider) {
      super(uri, null, provider, null, true);

      this.cm = cacheManager;
      this.icr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(InternalCacheRegistry.class);

      registerPredefinedCaches();
   }

   private void registerPredefinedCaches() {
      // TODO get predefined caches and register them
      // TODO galderz find a better way to do this as spec allows predefined caches to be
      // loaded (config file), instantiated and registered with CacheManager
      Set<String> cacheNames = cm.getCacheNames();
      for (String cacheName : cacheNames) {
         // With pre-defined caches, obey only pre-defined configuration
         if (icr.isInternalCache(cacheName))
            continue;

         registerPredefinedCache(cacheName, new JCache<>(
               cm.getCache(cacheName).getAdvancedCache(), this,
               ConfigurationAdapter.create()));
      }
   }

   private void loadConfigurationFromURI(ConfigurationBuilderHolder cbh) {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      try (
            InputStream configurationStream = getURI().isAbsolute()
                  ? fileLookup.lookupFileStrict(getURI(), cbh.getClassLoader())
                  : fileLookup.lookupFileStrict(getURI().toString(), cbh.getClassLoader())) {
         new ParserRegistry(cbh.getClassLoader()).parse(configurationStream, cbh, null, MediaType.fromExtension(getURI().toString()));
      } catch (IOException e) {
         // No such file, ignore for now (although we should probably handle this better in the future)
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
      // Unlike EmbeddedCacheManager, we do not list user-visible internal caches
      Set<String> cacheNames = new HashSet<>();
      for (String cacheName : cm.getCacheNames()) {
         if (!icr.isInternalCache(cacheName)) {
            cacheNames.add(cacheName);
         }
      }
      return Collections.unmodifiableSet(cacheNames);
   }

   @Override
   protected boolean delegateIsClosed() {
      return cm.getStatus().isTerminated();
   }

   @Override
   protected <K, V> void delegateRemoveCache(AbstractJCache<K, V> jcache) {
      String cacheName = jcache.getName();
      cm.administration().removeCache(cacheName);
   }

   @Override
   protected <K, V, C extends Configuration<K, V>> AbstractJCache<K, V> create(String cacheName, C configuration) {
      checkNotInternalCache(cacheName);
      org.infinispan.configuration.cache.Configuration baseConfig = null;
      if (properties != null && properties.containsKey(CACHE_CONFIGURATION_FUNCTION)) {
         Function<String, org.infinispan.configuration.cache.Configuration> f = (Function<String, org.infinispan.configuration.cache.Configuration>) properties.get(CACHE_CONFIGURATION_FUNCTION);
         baseConfig = f.apply(cacheName);
      }
      if (baseConfig == null) {
         baseConfig = cm.getCacheConfiguration(cacheName);
      }
      ConfigurationAdapter<K, V> adapter = ConfigurationAdapter.create(configuration);
      cm.defineConfiguration(cacheName, adapter.build(baseConfig));
      AdvancedCache<K, V> ispnCache =
            cm.<K, V>getCache(cacheName).getAdvancedCache();

      // In case the cache was stopped
      if (!ispnCache.getStatus().allowInvocations())
         ispnCache.start();

      return new JCache<>(ispnCache, this, adapter);
   }

   @Override
   protected <K, V, I extends BasicCache<K, V>> AbstractJCache<K, V> create(I ispnCache) {
      checkNotInternalCache(ispnCache.getName());

      return new JCache<>((AdvancedCache<K, V>) ispnCache, this, ConfigurationAdapter.create());
   }

   private void checkNotInternalCache(String cacheName) {
      if (icr.isInternalCache(cacheName))
         throw new IllegalArgumentException("Cache name " + cacheName +
               "is not allowed as it clashes with an internal cache");
   }
}
