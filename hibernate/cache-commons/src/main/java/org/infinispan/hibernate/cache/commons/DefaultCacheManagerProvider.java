package org.infinispan.hibernate.cache.commons;

import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_INFINISPAN_CONFIG_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.INFINISPAN_GLOBAL_STATISTICS_PROP;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.EmbeddedCacheManagerProvider;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Default {@link EmbeddedCacheManagerProvider} implementation.
 *
 * @author Paul Ferraro
 * @since 9.2
 */
public class DefaultCacheManagerProvider implements EmbeddedCacheManagerProvider {
   private static final InfinispanMessageLogger LOGGER = InfinispanMessageLogger.Provider.getLog(DefaultCacheManagerProvider.class);

   private final ServiceRegistry registry;

   public DefaultCacheManagerProvider(ServiceRegistry registry) {
      this.registry = registry;
   }

   @Override
   public EmbeddedCacheManager getEmbeddedCacheManager(Properties properties) {
      return new DefaultCacheManager(loadConfiguration(this.registry, properties), true);
   }

   // This is public for reuse by tests
   public static ConfigurationBuilderHolder loadConfiguration(ServiceRegistry registry, Properties properties) {
      String config = ConfigurationHelper.extractPropertyValue(INFINISPAN_CONFIG_RESOURCE_PROP, properties);
      ConfigurationBuilderHolder holder = loadConfiguration(registry, (config != null) ? config : DEF_INFINISPAN_CONFIG_RESOURCE);

      // Override statistics if enabled via properties
      String globalStatsProperty = ConfigurationHelper.extractPropertyValue(INFINISPAN_GLOBAL_STATISTICS_PROP, properties);
      if (globalStatsProperty != null) {
         holder.getGlobalConfigurationBuilder().globalJmxStatistics().enabled(Boolean.parseBoolean(globalStatsProperty));
      }

      return holder;
   }

   public static ConfigurationBuilderHolder loadConfiguration(ServiceRegistry registry, String config) {
      ClassLoaderService.Work<ConfigurationBuilderHolder> work = classLoader -> {
         ClassLoader infinispanClassLoader = InfinispanProperties.class.getClassLoader();
         try (InputStream input = lookupFile(config, classLoader, infinispanClassLoader)) {
            return parse(input, infinispanClassLoader);
         } catch (IOException e) {
            throw LOGGER.unableToCreateCacheManager(e);
         }
      };
      return registry.getService(ClassLoaderService.class).workWithClassLoader(work);
   }

   private static InputStream lookupFile(String configFile, ClassLoader classLoader, ClassLoader strictClassLoader) throws FileNotFoundException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream input = fileLookup.lookupFile(configFile, classLoader);
      // when it's not a user-provided configuration file, it might be a default configuration file,
      // and if that's included in [this] module might not be visible to the ClassLoaderService:
      if (input == null) {
         // This time use lookupFile*Strict* so to provide an exception if we can't find it yet:
         input = fileLookup.lookupFileStrict(configFile, strictClassLoader);
      }
      return input;
   }

   private static ConfigurationBuilderHolder parse(InputStream input, ClassLoader classLoader) {
      ParserRegistry parser = new ParserRegistry(classLoader);
      // Infinispan requires the context ClassLoader to have full visibility on all
      // its components and eventual extension points even *during* configuration parsing.
      Thread currentThread = Thread.currentThread();
      ClassLoader originalClassLoader = currentThread.getContextClassLoader();
      try {
         currentThread.setContextClassLoader(classLoader);
         ConfigurationBuilderHolder builderHolder = parser.parse(input);
         // Workaround Infinispan's ClassLoader strategies to bend to our will:
         builderHolder.getGlobalConfigurationBuilder().classLoader(classLoader);
         return builderHolder;
      }
      finally {
         currentThread.setContextClassLoader(originalClassLoader);
      }
   }
}
