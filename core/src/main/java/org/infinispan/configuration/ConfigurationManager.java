package org.infinispan.configuration;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

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
            .forEach((name, builder) -> putConfiguration(name, builder.build(globalConfiguration)));
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
      addAliases(cacheName, configuration.aliases(), false);
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
         removed.attributes().attribute(Configuration.ALIASES).removeListener(AliasListener.FILTER);
         removeAliases(removed.aliases());
      }
   }

   public Collection<String> getDefinedCaches() {
      return namedConfiguration.entrySet().stream()
            .filter(entry -> !entry.getValue().isTemplate())
            .map(Map.Entry::getKey).toList();
   }

   public Collection<String> getAliases() {
      return aliases.keySet();
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

   private Map<String, String> addAliases(String cacheName, Collection<String> aliases, boolean force) {
      synchronized (this.aliases) {
         for (String alias : aliases) {
            // Ensure there are no cache name conflicts
            if (this.namedConfiguration.containsKey(alias) || cacheName.equals(alias)) {
               throw CONFIG.duplicateCacheName(alias);
            }
            // Ensure there are no alias name conflicts
            if (!force && this.aliases.containsKey(alias)) {
               throw CONFIG.duplicateAliasName(alias, this.aliases.get(alias));
            }
         }
         Map<String, String> oldOwners = new HashMap<>();
         // Now we can register the aliases
         for (String alias : aliases) {
            String old = this.aliases.put(alias, cacheName);
            if (old != null) {
               oldOwners.put(alias, old);
            }
         }
         return oldOwners;
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

   class AliasListener implements AttributeListener<Set<String>> {
      static final Predicate<AttributeListener<Set<String>>> FILTER = f -> f.getClass() == AliasListener.class;
      private final String cacheName;

      AliasListener(String cacheName) {
         this.cacheName = cacheName;
      }

      public void attributeChanged(Attribute<Set<String>> attribute, Set<String> oldValues) {
         Map<String, String> oldOwners;
         synchronized (aliases) {
            // Ensure that any new aliases aren't registered yet
            List<String> newValues = new ArrayList<>(attribute.get());
            newValues.removeAll(oldValues);
            oldOwners = addAliases(cacheName, newValues, true);
            // Now remove the ones that are no longer needed
            List<String> removedValues = new ArrayList<>(oldValues);
            removedValues.removeAll(attribute.get());
            removeAliases(removedValues);
         }
         // alter all the old owners, removing the aliases from their configuration
         for (Map.Entry<String, String> oldOwner : oldOwners.entrySet()) {
            String otherCacheName = oldOwner.getValue();
            Configuration otherConfiguration = ConfigurationManager.this.namedConfiguration.get(otherCacheName);
            Attribute<Set<String>> otherAliases = otherConfiguration.attributes().attribute(Configuration.ALIASES);
            // Remove the other listener
            otherAliases.removeListener(FILTER);
            Set<String> list = new HashSet<>(otherAliases.get());
            list.remove(oldOwner.getKey());
            otherAliases.set(list);
            // Reinstall the listener
            otherAliases.addListener(new AliasListener(otherCacheName));
         }
      }
   }
}
