package org.infinispan.registry.impl;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * InternalCacheRegistryImpl.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class InternalCacheRegistryImpl implements InternalCacheRegistry {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private EmbeddedCacheManager cacheManager;
   private final Set<String> internalCaches = new ConcurrentHashSet<>();
   private final Set<String> privateCaches = new ConcurrentHashSet<>();

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public void registerInternalCache(String name, Configuration configuration) {
      registerInternalCache(name, configuration, EnumSet.noneOf(Flag.class));
   }

   @Override
   public void registerInternalCache(String name, Configuration configuration, EnumSet<Flag> flags) {
      // check if it already has been defined. Currently we don't support existing user-defined configuration.
      if ((flags.contains(Flag.EXCLUSIVE) || !internalCaches.contains(name)) && cacheManager.getCacheConfiguration(name) != null) {
         throw log.existingConfigForInternalCache(name);
      }
      ConfigurationBuilder builder = new ConfigurationBuilder().read(configuration);
      builder.jmxStatistics().disable(); // Internal caches must not be included in stats counts
      SecurityActions.defineConfiguration(cacheManager, name, configuration);
      internalCaches.add(name);
      if (!flags.contains(Flag.USER)) {
         privateCaches.add(name);
      }
   }

   @Override
   public boolean isInternalCache(String name) {
      return internalCaches.contains(name);
   }

   @Override
   public Set<String> getInternalCacheNames() {
      return internalCaches;
   }

   @Override
   public void filterInternalCaches(Set<String> names) {
      names.removeAll(privateCaches);
   }
}
