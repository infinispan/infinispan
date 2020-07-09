package org.infinispan.configuration;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.commons.util.GlobUtils;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * It manages all the configuration for a specific container.
 * <p>
 * It manages the {@link GlobalConfiguration}, the default {@link Configuration} and all the defined named caches {@link
 * Configuration}.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public class ConfigurationManager {
   private final ConcurrentMap<String, Configuration> namedConfiguration;
   @Inject GlobalComponentRegistry gcr;

   public ConfigurationManager() {
      this.namedConfiguration = new ConcurrentHashMap<>();
   }

   public GlobalConfiguration getGlobalConfiguration() {
      return gcr.getGlobalConfiguration();
   }

   public void registerConfigurations(ConfigurationBuilderHolder holder) {
      holder.getNamedConfigurationBuilders().forEach((name, builder) -> namedConfiguration.put(name, builder.build(getGlobalConfiguration())));
   }

   public void registerDefaultConfiguration(String name, Configuration configuration) {
      namedConfiguration.put(name, configuration);
   }

   public void removeInvalids() {
      namedConfiguration.entrySet().removeIf(this::isInvalid);
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
      gcr.notifyCreatingConfiguration(configuration);
      namedConfiguration.put(cacheName, configuration);
      return configuration;
   }

   public Configuration putConfiguration(String cacheName, ConfigurationBuilder builder) {
      return putConfiguration(cacheName, builder.build(getGlobalConfiguration()));
   }

   public void removeConfiguration(String cacheName) {
      namedConfiguration.remove(cacheName);
   }

   public Collection<String> getDefinedCaches() {
      List<String> cacheNames = namedConfiguration.entrySet().stream()
            .filter(entry -> !entry.getValue().isTemplate())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
      return Collections.unmodifiableCollection(cacheNames);
   }

   public Collection<String> getDefinedConfigurations() {
      return Collections.unmodifiableCollection(namedConfiguration.keySet());
   }

   private boolean isInvalid(Map.Entry<String, Configuration> entry) {
      try {
         gcr.notifyCreatingConfiguration(entry.getValue());
         return false;
      } catch (Throwable t) {
         //it throws an exception if it isn't valid
         CONFIG.removedInvalidConfiguration(entry.getKey(), t);
         return true;
      }
   }
}
