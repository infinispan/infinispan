package org.infinispan.globalstate.impl;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An implementation of {@link LocalConfigurationStorage} which does not support
 * {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#PERMANENT} operations
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class VolatileLocalConfigurationStorage implements LocalConfigurationStorage {
   protected static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected EmbeddedCacheManager cacheManager;
   protected ParserRegistry parserRegistry;
   protected GlobalConfiguration globalConfiguration;

   public VolatileLocalConfigurationStorage() {
   }

   public void initialize(EmbeddedCacheManager cacheManager) {
      this.globalConfiguration = cacheManager.getCacheManagerConfiguration();
      this.cacheManager = cacheManager;
      this.parserRegistry = new ParserRegistry();
   }

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT))
         throw log.globalStateDisabled();
   }

   public void createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration existing = cacheManager.getCacheConfiguration(name);
      if (existing == null) {
         cacheManager.defineConfiguration(name, configuration);
         log.debugf("Defined cache '%s' on '%s' using %s", name, cacheManager.getAddress(), configuration);
      } else if (!existing.matches(configuration)) {
         throw log.incompatibleClusterConfiguration(name, configuration, existing);
      } else {
         log.debugf("%s already has a cache %s with configuration %s", cacheManager.getAddress(), name, configuration);
      }
      // Ensure the cache is started
      cacheManager.getCache(name);
   }


   public void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      log.debugf("Remove cache %s", name);

      GlobalComponentRegistry globalComponentRegistry = cacheManager.getGlobalComponentRegistry();
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(name);
      if (cacheComponentRegistry != null) {
         cacheComponentRegistry.getComponent(PersistenceManager.class).setClearOnStop(true);
         cacheComponentRegistry.getComponent(CacheJmxRegistration.class).setUnregisterCacheMBean(true);
         cacheComponentRegistry.getComponent(PassivationManager.class).skipPassivationOnStop(true);
         Cache<?, ?> cache = cacheManager.getCache(name, false);
         if (cache != null) {
            cache.stop();
         }
      }
      globalComponentRegistry.removeCache(name);
      // Remove cache configuration and remove it from the computed cache name list
      globalComponentRegistry.getComponent(ConfigurationManager.class).removeConfiguration(name);
      // Remove cache from dependency graph
      //noinspection unchecked
      globalComponentRegistry.getComponent(DependencyGraph.class, CACHE_DEPENDENCY_GRAPH).remove(name);
   }

   public Map<String, Configuration> loadAll() {
      // This is volatile, so nothing to do here
      return Collections.emptyMap();
   }
}
