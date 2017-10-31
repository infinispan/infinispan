package org.infinispan.globalstate.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileLock;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of {@link GlobalConfigurationManager}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class GlobalConfigurationManagerImpl implements GlobalConfigurationManager {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private GlobalConfiguration globalConfiguration;
   private EmbeddedCacheManager cacheManager;
   private Cache<ScopedState, Object> stateCache;
   public static final String CACHE_SCOPE = "cache";
   private ParserRegistry parserRegistry;
   private ConcurrentHashSet<String> persistentCaches;

   @Inject
   public void inject(GlobalConfiguration globalConfiguration, EmbeddedCacheManager cacheManager) {
      this.globalConfiguration = globalConfiguration;
      this.cacheManager = cacheManager;
   }

   @Start
   public void start() {
      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(
            CONFIG_STATE_CACHE_NAME,
            new ConfigurationBuilder().build(),
            EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
      parserRegistry = new ParserRegistry();
      persistentCaches = new ConcurrentHashSet<>();
   }

   @PostStart
   public void postStart() {
      // Initialize caches which are present in the initial state
      for (Map.Entry<ScopedState, Object> e : getStateCache().entrySet()) {
         if (CACHE_SCOPE.equals(e.getKey().getScope()))
            localCreateCache(e.getKey().getName(), (CacheState) e.getValue());
      }
      // Install the listener
      GlobalConfigurationStateListener stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener, new ScopeFilter(CACHE_SCOPE));

      if (globalConfiguration.globalState().enabled()) {
         // Load any persisted configs
         try (FileInputStream fis = new FileInputStream(getPersistentFile())) {
            ConfigurationBuilderHolder holder = parserRegistry.parse(fis);
            for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
               String name = entry.getKey();
               Configuration configuration = entry.getValue().build();
               // The cache was persisted, it still needs to be
               createCache(name, configuration, EnumSet.of(CacheContainerAdmin.AdminFlag.PERSISTENT));
            }
         } catch (FileNotFoundException e) {
            // Ignore
         } catch (IOException e) {
            throw new CacheConfigurationException(e);
         }
      }
   }

   private File getPersistentFile() {
      return new File(globalConfiguration.globalState().persistentLocation(), "caches.xml");
   }

   private File getPersistentFileLock() {
      return new File(globalConfiguration.globalState().persistentLocation(), "caches.xml.lck");
   }

   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   private void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERSISTENT) && !globalConfiguration.globalState().enabled())
         throw log.globalStateDisabled();
   }

   @Override
   public Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      validateFlags(flags);
      try {
         CacheState state = new CacheState(parserRegistry.serialize(cacheName, configuration), flags);
         getStateCache().putIfAbsent(new ScopedState(CACHE_SCOPE, cacheName), state);
         return configuration;
      } catch (Exception e) {
         throw log.configurationSerializationFailed(cacheName, configuration, e);
      }
   }

   @Override
   public Configuration createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration configuration;
      if (template == null) {
         configuration = cacheManager.getDefaultCacheConfiguration();
         if (configuration == null)
            configuration = new ConfigurationBuilder().build();
      } else {
         configuration = cacheManager.getCacheConfiguration(template);
         if (configuration == null)
            throw log.undeclaredConfiguration(cacheName, template);
      }
      return createCache(cacheName, configuration, flags);
   }

   void localCreateCache(String name, CacheState state) {
      log.debugf("Create cache %s", name);
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(state.getConfiguration());
      Configuration configuration = builderHolder.getNamedConfigurationBuilders().get(name).build();
      localCreateCache(name, configuration, state.getFlags());
   }

   void localCreateCache(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration existing = cacheManager.getCacheConfiguration(name);
      if (existing == null) {
         cacheManager.defineConfiguration(name, configuration);
         log.debugf("Defined cache '%s' on '%s' using %s", name, cacheManager.getAddress(), configuration);
      } else if (!existing.equals(configuration)) {
         throw log.incompatibleClusterConfiguration(name, configuration, existing);
      } else {
         log.debugf("%s already has a cache %s with configuration %s", cacheManager.getAddress(), name, configuration);
      }
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERSISTENT)) {
         persistentCaches.add(name);
         persistConfigurations();
      }
      cacheManager.getCache(name);
   }

   @Override
   public void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      validateFlags(flags);
      if (flags.contains(CacheContainerAdmin.AdminFlag.PERSISTENT)) {
         persistentCaches.remove(name);
         persistConfigurations();
      }
      ScopedState cacheScopedState = new ScopedState(CACHE_SCOPE, name);
      if (getStateCache().containsKey(cacheScopedState)) {
         getStateCache().remove(cacheScopedState);
      } else {
         localRemoveCache(name);
      }
   }

   void localRemoveCache(String cacheName) {
      log.debugf("Remove cache %s", cacheName);
      RemoveCacheCommand.removeCache(cacheManager, cacheName);
   }

   private void persistConfigurations() {
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
            temp.renameTo(getPersistentFile());
         } finally {
            if (lock != null && lock.isValid())
               lock.release();
            getPersistentFileLock().delete();
         }

      } catch (Exception e) {
         throw log.errorPersistingGlobalConfiguration(e);
      }
   }
}
