package org.infinispan.configuration;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeListener;
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
   private final Map<String, Configuration> namedConfiguration;
   private final Map<String, String> aliases;


   public ConfigurationManager(GlobalConfiguration globalConfiguration) {
      this.globalConfiguration = globalConfiguration;
      this.namedConfiguration = new ConcurrentHashMap<>();
      this.aliases = new ConcurrentHashMap<>();
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
      // This will fail if the names are already in use
      addAliases(cacheName, configuration.aliases());
      namedConfiguration.put(cacheName, configuration);
      configuration.attributes().attribute(Configuration.ALIASES).addListener(new AliasListener(cacheName));
      return configuration;
   }

   public Configuration putConfiguration(String cacheName, ConfigurationBuilder builder) {
      return putConfiguration(cacheName, builder.build(globalConfiguration));
   }

   public void removeConfiguration(String cacheName) {
      Configuration removed = namedConfiguration.remove(cacheName);
      if (removed != null) {
         removed.attributes().attribute(Configuration.ALIASES).removeListener(f -> f.getClass() == AliasListener.class);
         removeAliases(removed.aliases());
      }
   }

   public Collection<String> getDefinedCaches() {
      return namedConfiguration.entrySet().stream()
            .filter(entry -> !entry.getValue().isTemplate())
            .map(Map.Entry::getKey).toList();
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

   private void addAliases(String cacheName, Collection<String> aliases) {
      synchronized (this.aliases) {
         for (String alias : aliases) {
            // Ensure there are no cache name conflicts
            if (this.namedConfiguration.containsKey(alias) || cacheName.equals(alias)) {
               throw CONFIG.duplicateCacheName(alias);
            }
            // Ensure there are no alias name conflicts
            if (this.aliases.containsKey(alias)) {
               throw CONFIG.duplicateAliasName(alias, this.aliases.get(alias));
            }
         }
         // Now we can register the aliases
         for (String alias : aliases) {
            this.aliases.put(alias, cacheName);
         }
      }
   }

   private void removeAliases(Collection<String> aliases) {
      for (String alias : aliases) {
         this.aliases.remove(alias);
      }
   }

   public String selectCache(String cacheName) {
      return aliases.getOrDefault(cacheName, cacheName);
   }

   class AliasListener implements AttributeListener<List<String>> {
      private final String cacheName;

      AliasListener(String cacheName) {
         this.cacheName = cacheName;
      }

      public void attributeChanged(Attribute<List<String>> attribute, List<String> oldValues) {
         synchronized (aliases) {
            // Ensure that any new aliases aren't registered yet
            List<String> newValues = new ArrayList<>(attribute.get());
            newValues.removeAll(oldValues);
            addAliases(cacheName, newValues);
            // Now remove the ones that are no longer needed
            List<String> removedValues = new ArrayList<>(oldValues);
            removedValues.removeAll(attribute.get());
            removeAliases(removedValues);
         }
      }
   }
}
