package org.infinispan.configuration;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;

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
   private final ConcurrentMap<String, Configuration> namedConfiguration;


   public ConfigurationManager(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
      this.namedConfiguration = new ConcurrentHashMap<>();
   }

   public ConfigurationManager(ConfigurationBuilderHolder holder) {
      this(holder.getGlobalConfigurationBuilder().build());

      holder.getNamedConfigurationBuilders()
            .forEach((name, builder) -> namedConfiguration.put(name, builder.build(globalConfiguration)));
   }

   public GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   public Configuration getConfiguration(String cacheName, boolean includeWildcards) {
      if (includeWildcards)
         return findConfiguration(cacheName);
      else
         return namedConfiguration.get(cacheName);
   }

   public Configuration getConfiguration(String cacheName) {
      Configuration configuration = findConfiguration(cacheName);
      if (configuration != null) {
         return configuration;
      } else {
         throw CONFIG.noSuchCacheConfiguration(cacheName);
      }
   }

   private Configuration findConfiguration(String name) {
      if (namedConfiguration.containsKey(name)) {
         return namedConfiguration.get(name);
      } else {
         // Attempt wildcard matching
         Configuration match = null;
         for (Map.Entry<String, Configuration> c : namedConfiguration.entrySet()) {
            String key = c.getKey();
            if (GlobUtils.isGlob(key)) {
               if (name.matches(GlobUtils.globToRegex(key))) {
                  if (match == null) {
                     match = c.getValue();
                     // If this is a template, turn it into a concrete configuration
                     if (match.isTemplate()) {
                        ConfigurationBuilder builder = new ConfigurationBuilder().read(match).template(false);
                        match = builder.build();
                     }
                  } else
                     throw CONFIG.configurationNameMatchesMultipleWildcards(name);
               }
            }
         }
         return match;
      }
   }

   public Configuration putConfiguration(String cacheName, Configuration configuration) {
      namedConfiguration.put(cacheName, configuration);
      return configuration;
   }

   public Configuration putConfiguration(String cacheName, ConfigurationBuilder builder) {
      return putConfiguration(cacheName, builder.build(globalConfiguration));
   }

   public void removeConfiguration(String cacheName) {
      namedConfiguration.remove(cacheName);
   }

   public Collection<String> getDefinedCaches() {
      return namedConfiguration.entrySet().stream()
            .filter(entry -> !entry.getValue().isTemplate())
            .map(Map.Entry::getKey).collect(Collectors.toUnmodifiableList());
   }

   public Collection<String> getDefinedConfigurations() {
      return Collections.unmodifiableCollection(namedConfiguration.keySet());
   }

   public ConfigurationBuilderHolder toBuilderHolder() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().read(globalConfiguration);
      for (Map.Entry<String, Configuration> entry : namedConfiguration.entrySet()) {
         holder.newConfigurationBuilder(entry.getKey()).read(entry.getValue(), Combine.DEFAULT);
      }
      return holder;
   }
}
