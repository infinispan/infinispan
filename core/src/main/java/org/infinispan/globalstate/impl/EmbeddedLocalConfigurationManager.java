package org.infinispan.globalstate.impl;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.LocalConfigurationManager;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default implementation of {@link LocalConfigurationManager} for embedded scenarios.
 *
 * This component persists cache configurations to the {@link GlobalStateConfiguration#persistentLocation()} in a
 * <pre>caches.xml</pre> file which is read on startup.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class EmbeddedLocalConfigurationManager implements LocalConfigurationManager {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private ConcurrentHashSet<String> persistentCaches = new ConcurrentHashSet<>();
   private EmbeddedCacheManager cacheManager;
   private ParserRegistry parserRegistry;
   private GlobalConfiguration globalConfiguration;

   public EmbeddedLocalConfigurationManager() {
   }

   public void initialize(EmbeddedCacheManager cacheManager, GlobalConfigurationManager globalConfigurationManager) {
      this.globalConfiguration = cacheManager.getCacheManagerConfiguration();
      this.cacheManager = cacheManager;
      this.parserRegistry = new ParserRegistry();
   }

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT) && !globalConfiguration.globalState().enabled())
         throw log.globalStateDisabled();
   }

   public void createCache(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration existing = cacheManager.getCacheConfiguration(name);
      if (existing == null) {
         cacheManager.defineConfiguration(name, configuration);
         log.debugf("Defined cache '%s' on '%s' using %s", name, cacheManager.getAddress(), configuration);
      } else if (!existing.matches(configuration)) {
         throw log.incompatibleClusterConfiguration(name, configuration, existing);
      } else {
         log.debugf("%s already has a cache %s with configuration %s", cacheManager.getAddress(), name, configuration);
      }
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERMANENT)) {
         persistentCaches.add(name);
         storeAll();
      }
      // Ensure the cache is started
      cacheManager.getCache(name);
   }


   public void removeCache(String name) {
      log.debugf("Remove cache %s", name);
      if (persistentCaches.remove(name)) {
         storeAll();
      }

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
      // Load any persisted configs

      try (FileInputStream fis = new FileInputStream(getPersistentFile())) {
         Map<String, Configuration> configurations = new HashMap<>();
         ConfigurationBuilderHolder holder = parserRegistry.parse(fis);
         for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            String name = entry.getKey();
            Configuration configuration = entry.getValue().build();
            configurations.put(name, configuration);
         }
         return configurations;
      } catch (FileNotFoundException e) {
         // Ignore
         return Collections.emptyMap();
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void storeAll() {
      try {
         File temp = File.createTempFile("caches", null, new File(globalConfiguration.globalState().persistentLocation()));
         Map<String, Configuration> configurationMap = new HashMap<>();
         for (String cacheName : persistentCaches) {
            configurationMap.put(cacheName, cacheManager.getCacheConfiguration(cacheName));
         }
         try (FileOutputStream f = new FileOutputStream(temp)) {
            parserRegistry.serialize(f, null, configurationMap);
         }
         FileLock lock = null;
         try (FileOutputStream lockFile = new FileOutputStream(getPersistentFileLock())) {
            lock = lockFile.getChannel().lock();
            if (!temp.renameTo(getPersistentFile())) {
               throw log.cannotRenamePersistentFile(temp.getAbsolutePath(), getPersistentFile());
            }
         } finally {
            if (lock != null && lock.isValid())
               lock.release();
            getPersistentFileLock().delete();
         }

      } catch (Exception e) {
         throw log.errorPersistingGlobalConfiguration(e);
      }
   }

   private File getPersistentFile() {
      return new File(globalConfiguration.globalState().persistentLocation(), "caches.xml");
   }

   private File getPersistentFileLock() {
      return new File(globalConfiguration.globalState().persistentLocation(), "caches.xml.lck");
   }
}
