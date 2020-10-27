package org.infinispan.globalstate.impl;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An implementation of {@link LocalConfigurationStorage} which does only supports
 * {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE} operations
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class VolatileLocalConfigurationStorage implements LocalConfigurationStorage {
   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected EmbeddedCacheManager cacheManager;
   protected ParserRegistry parserRegistry;
   protected ConfigurationManager configurationManager;
   protected BlockingManager blockingManager;

   @Override
   public void initialize(EmbeddedCacheManager cacheManager, ConfigurationManager configurationManager, BlockingManager blockingManager) {
      this.configurationManager = configurationManager;
      this.cacheManager = cacheManager;
      this.parserRegistry = new ParserRegistry();
      this.blockingManager = blockingManager;
   }

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE))
         throw CONFIG.globalStateDisabled();
   }

   @Override
   public CompletableFuture<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration existing = SecurityActions.getCacheConfiguration(cacheManager, name);
      if (existing == null) {
         SecurityActions.defineConfiguration(cacheManager, name, configuration);
         log.debugf("Defined template '%s' on '%s' using %s", name, cacheManager.getAddress(), configuration);
      } else if (!existing.matches(configuration)) {
         throw CONFIG.incompatibleClusterConfiguration(name, configuration, existing);
      } else {
         log.debugf("%s already has a template %s with configuration %s", cacheManager.getAddress(), name, configuration);
      }
      return CompletableFutures.completedNull();
   }

   public CompletableFuture<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration existing = SecurityActions.getCacheConfiguration(cacheManager, name);
      if (existing == null) {
         SecurityActions.defineConfiguration(cacheManager, name, configuration);
         log.debugf("Defined cache '%s' on '%s' using %s", name, cacheManager.getAddress(), configuration);
      } else if (!existing.matches(configuration)) {
         throw CONFIG.incompatibleClusterConfiguration(name, configuration, existing);
      } else {
         log.debugf("%s already has a cache %s with configuration %s", cacheManager.getAddress(), name, configuration);
      }
      // Ensure the cache is started
      return blockingManager.<Void>supplyBlocking(() -> {
         try {
            SecurityActions.getCache(cacheManager, name);
         } catch (CacheException cacheException) {
            log.cannotObtainFailedCache(name, cacheException);
         }
         return null;
      }, name).toCompletableFuture();
   }


   public CompletableFuture<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return blockingManager.<Void>supplyBlocking(() -> {
         removeCacheSync(name, flags);
         return null;
      }, name).toCompletableFuture();
   }

   protected void removeCacheSync(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      log.debugf("Remove cache %s", name);
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(name);
      if (cacheComponentRegistry != null) {
         cacheComponentRegistry.getComponent(PersistenceManager.class).setClearOnStop(true);
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

   @Override
   public CompletableFuture<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      removeTemplateSync(name, flags);
      return CompletableFutures.completedNull();
   }

   protected void removeTemplateSync(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      log.debugf("Remove template %s", name);
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      // Remove cache configuration and remove it from the computed cache name list
      globalComponentRegistry.getComponent(ConfigurationManager.class).removeConfiguration(name);
   }

   @Override
   public Map<String, Configuration> loadAllCaches() {
      // This is volatile, so nothing to do here
      return Collections.emptyMap();
   }

   @Override
   public Map<String, Configuration> loadAllTemplates() {
      // This is volatile, so nothing to do here
      return Collections.emptyMap();
   }
}
