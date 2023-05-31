package org.infinispan.registry.impl;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * InternalCacheRegistryImpl.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Scope(Scopes.GLOBAL)
public class InternalCacheRegistryImpl implements InternalCacheRegistry {
   private static final Log log = LogFactory.getLog(InternalCacheRegistryImpl.class);

   @Inject EmbeddedCacheManager cacheManager;
   @Inject CacheManagerJmxRegistration cacheManagerJmxRegistration;
   @Inject ConfigurationManager configurationManager;
   @Inject GlobalConfiguration globalConfiguration;

   private final ConcurrentMap<String, EnumSet<Flag>> internalCaches = new ConcurrentHashMap<>();
   private final Set<String> privateCaches = ConcurrentHashMap.newKeySet();

   @Override
   public void registerInternalCache(String name, Configuration configuration) {
      registerInternalCache(name, configuration, EnumSet.noneOf(Flag.class));
   }

   // Synchronized to prevent users from registering the same configuration at the same time
   @Override
   public synchronized void registerInternalCache(String name, Configuration configuration, EnumSet<Flag> flags) {
      log.debugf("Registering internal cache %s %s", name, flags);
      boolean configPresent = configurationManager.getConfiguration(name, false) != null;
      // check if it already has been defined. Currently we don't support existing user-defined configuration.
      if ((flags.contains(Flag.EXCLUSIVE) || !internalCaches.containsKey(name)) && configPresent) {
         throw CONFIG.existingConfigForInternalCache(name);
      }
      // Don't redefine
      if (configPresent) {
         return;
      }
      ConfigurationBuilder builder = new ConfigurationBuilder().read(configuration, Combine.DEFAULT);
      builder.statistics().disable(); // Internal caches must not be included in stats counts
      if (flags.contains(Flag.GLOBAL) && globalConfiguration.isClustered()) {
         // TODO: choose a merge policy
         builder.clustering()
               .cacheMode(CacheMode.REPL_SYNC)
               .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(true);
      }
      if (flags.contains(Flag.PERSISTENT)) {
         if (globalConfiguration.globalState().enabled()) {
            builder.persistence()
                  .availabilityInterval(-1)
                  .addSingleFileStore()
                     .location(globalConfiguration.globalState().persistentLocation())
                     // Internal caches don't need to be segmented
                     .segmented(false)
                     .purgeOnStartup(false)
                     .preload(true)
                     .fetchPersistentState(true);
         } else {
            CONFIG.warnUnableToPersistInternalCaches();
         }
      }
      SecurityActions.defineConfiguration(cacheManager, name, builder.build());
      internalCaches.put(name, flags);
      if (!flags.contains(Flag.USER)) {
         privateCaches.add(name);
      }
   }

   @Override
   public synchronized void unregisterInternalCache(String name) {
      log.debugf("Unregistering internal cache %s", name);
      if (isInternalCache(name)) {
         Cache<Object, Object> cache = cacheManager.getCache(name, false);
         if (cache != null) {
            cache.stop();
         }
         internalCaches.remove(name);
         privateCaches.remove(name);
         SecurityActions.undefineConfiguration(cacheManager, name);
      }
   }

   @Override
   public boolean isInternalCache(String name) {
      return internalCaches.containsKey(name);
   }

   @Override
   public boolean isPrivateCache(String name) {
      return privateCaches.contains(name);
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

   @Override
   public void startInternalCaches() {
      log.debugf("Starting internal caches: %s", internalCaches.keySet());
      for (String cacheName : internalCaches.keySet()) {
         SecurityActions.getCache(cacheManager, cacheName);
      }
   }
}
