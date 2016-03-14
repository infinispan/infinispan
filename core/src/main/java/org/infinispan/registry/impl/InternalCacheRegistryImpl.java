package org.infinispan.registry.impl;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
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
   private final ConcurrentMap<String, EnumSet<Flag>> internalCaches = CollectionFactory.makeBoundedConcurrentMap(10);
   private final Set<String> privateCaches = new ConcurrentHashSet<>();

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Stop(priority = 1)
   public void stop() {
      internalCaches.keySet().forEach(cacheName -> {
         Cache<Object, Object> cache = cacheManager.getCache(cacheName, false);
         if (cache != null)
            cache.stop();
      });
   }

   @Override
   public void registerInternalCache(String name, Configuration configuration) {
      registerInternalCache(name, configuration, EnumSet.noneOf(Flag.class));
   }

   @Override
   public void registerInternalCache(String name, Configuration configuration, EnumSet<Flag> flags) {
      // check if it already has been defined. Currently we don't support existing user-defined configuration.
      if ((flags.contains(Flag.EXCLUSIVE) || !internalCaches.containsKey(name)) && cacheManager.getCacheConfiguration(name) != null) {
         throw log.existingConfigForInternalCache(name);
      }
      ConfigurationBuilder builder = new ConfigurationBuilder().read(configuration);
      builder.jmxStatistics().disable(); // Internal caches must not be included in stats counts
      GlobalConfiguration globalConfiguration = cacheManager.getCacheManagerConfiguration();
      if (flags.contains(Flag.PERSISTENT) && globalConfiguration.globalState().enabled()) {
         builder.persistence().addSingleFileStore().location(globalConfiguration.globalState().persistentLocation()).purgeOnStartup(false).preload(true);
      }
      internalCaches.put(name, flags);
      SecurityActions.defineConfiguration(cacheManager, name, builder.build());
      if (!flags.contains(Flag.USER)) {
         privateCaches.add(name);
      }
   }

   @Override
   public boolean isInternalCache(String name) {
      return internalCaches.containsKey(name);
   }

   @Override
   public Set<String> getInternalCacheNames() {
      return internalCaches.keySet();
   }

   @Override
   public void filterPrivateCaches(Set<String> names) {
      names.removeAll(privateCaches);
   }

   @Override
   public boolean internalCacheHasFlag(String name, Flag flag) {
      EnumSet<Flag> flags = internalCaches.get(name);
      return flags != null && flags.contains(flag);
   }
}
