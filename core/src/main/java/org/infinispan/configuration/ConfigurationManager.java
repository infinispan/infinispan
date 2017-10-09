package org.infinispan.configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.GlobUtils;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private static final Log log = LogFactory.getLog(ConfigurationManager.class);

   private final GlobalConfiguration globalConfiguration;
   private final ConcurrentMap<String, Configuration> namedConfiguration;


   public ConfigurationManager(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
      this.namedConfiguration = CollectionFactory.makeConcurrentMap();
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

   public Configuration getConfiguration(String cacheName, String defaultCacheName) {
      Configuration configuration = findConfiguration(cacheName);
      if (configuration != null)
         return configuration;
      if (defaultCacheName != null) {
         return namedConfiguration.get(defaultCacheName);
      } else {
         throw log.noSuchCacheConfiguration(cacheName);
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
                  if (match == null)
                     match = c.getValue();
                  else
                     throw log.configurationNameMatchesMultipleWildcards(name);
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
      return putConfiguration(cacheName, builder.build());
   }

   public void removeConfiguration(String cacheName) {
      namedConfiguration.remove(cacheName);
   }

   public Collection<String> getDefinedCaches() {
      List<String> cacheNames = namedConfiguration.entrySet().stream()
            .filter(entry -> !entry.getValue().isTemplate() && !entry.getKey().equals(CacheContainer.DEFAULT_CACHE_NAME))
            .map(entry -> entry.getKey())
            .collect(Collectors.toList());
      return Collections.unmodifiableCollection(cacheNames);
   }

   public Collection<String> getDefinedConfigurations() {
      return Collections.unmodifiableCollection(namedConfiguration.keySet());
   }
}
