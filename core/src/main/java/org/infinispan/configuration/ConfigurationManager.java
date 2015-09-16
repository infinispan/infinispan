package org.infinispan.configuration;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;

/**
 * It manages all the configuration for a specific container.
 * <p>
 * It manages the {@link GlobalConfiguration}, the default {@link Configuration} and all the defined named caches {@link
 * Configuration}.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
public class ConfigurationManager {

   private final GlobalConfiguration globalConfiguration;
   private final Configuration defaultConfiguration;
   private final ConcurrentMap<String, Configuration> namedConfiguration;

   public ConfigurationManager(ConfigurationBuilderHolder globalConfigurationHolder, ConfigurationBuilderHolder defaultConfigurationHolder,
                               Optional<ConfigurationBuilderHolder> namedConfigurationHolder) {
      globalConfiguration = globalConfigurationHolder.getGlobalConfigurationBuilder().build();
      defaultConfiguration = defaultConfigurationHolder.getDefaultConfigurationBuilder().build(globalConfiguration);
      namedConfiguration = CollectionFactory.makeConcurrentMap();

      if (namedConfigurationHolder.isPresent()) {
         for (Map.Entry<String, ConfigurationBuilder> entry : namedConfigurationHolder.get().getNamedConfigurationBuilders().entrySet()) {
            ConfigurationBuilder builder = entry.getValue();
            org.infinispan.configuration.cache.Configuration c = builder.build(globalConfiguration);
            namedConfiguration.put(entry.getKey(), c);
         }
      }
   }

   public ConfigurationManager(ConfigurationBuilderHolder holder) {
      this(holder, holder, Optional.of(holder));
   }

   public ConfigurationManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration) {
      this.globalConfiguration = globalConfiguration;
      this.defaultConfiguration = defaultConfiguration;
      this.namedConfiguration = CollectionFactory.makeConcurrentMap();
   }

   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   public Configuration getDefaultConfiguration() {
      return defaultConfiguration;
   }

   public Configuration getConfiguration(String cacheName) {
      return DEFAULT_CACHE_NAME.equals(cacheName) ? defaultConfiguration : namedConfiguration.get(cacheName);
   }

   public Configuration getConfigurationOrDefault(String cacheName) {
      if (DEFAULT_CACHE_NAME.equals(cacheName) || !namedConfiguration.containsKey(cacheName)) {
         return new ConfigurationBuilder().read(defaultConfiguration).build(globalConfiguration);
      } else {
         return namedConfiguration.get(cacheName);
      }
   }

   public Configuration putConfiguration(String cacheName, ConfigurationBuilder builder) {
      Configuration configuration = builder.build(globalConfiguration);
      namedConfiguration.put(cacheName, configuration);
      return configuration;
   }

   public void removeConfiguration(String cacheName) {
      namedConfiguration.remove(cacheName);
   }

   public Collection<String> getDefinedCaches() {
      return Collections.unmodifiableCollection(namedConfiguration.keySet());
   }
}
