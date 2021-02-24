package org.infinispan.globalstate.impl;

import static org.infinispan.commons.util.Util.renameTempFile;
import static org.infinispan.util.logging.Log.CONFIG;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.globalstate.LocalConfigurationStorage;

/**
 * An implementation of {@link LocalConfigurationStorage} which stores non-{@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
 * <p>
 * This component persists cache configurations to the {@link GlobalStateConfiguration#persistentLocation()} in a
 * <pre>caches.xml</pre> file which is read on startup.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class OverlayLocalConfigurationStorage extends VolatileLocalConfigurationStorage {
   private Set<String> persistentCaches = ConcurrentHashMap.newKeySet();
   private final Lock persistenceLock = new ReentrantLock();

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE) && !globalConfiguration.globalState().enabled())
         throw CONFIG.globalStateDisabled();
   }

   public CompletableFuture<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletableFuture<Void> future = super.createCache(name, template, configuration, flags);
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE)) {
         return blockingManager.thenApplyBlocking(future, (v) -> {
            persistentCaches.add(name);
            storeAll();
            return v;
         }, name).toCompletableFuture();
      } else {
         return future;
      }
   }

   public CompletableFuture<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return blockingManager.<Void>supplyBlocking(() -> {
         if (persistentCaches.remove(name)) {
            storeAll();
         }
         removeCacheSync(name, flags);
         return null;
      }, name).toCompletableFuture();
   }

   public Map<String, Configuration> loadAll() {
      // Load any persisted configs

      try (FileInputStream fis = new FileInputStream(getPersistentFile())) {
         Map<String, Configuration> configurations = new HashMap<>();
         ConfigurationBuilderHolder holder = parserRegistry.parse(fis, null);
         for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            String name = entry.getKey();
            Configuration configuration = entry.getValue().build();
            configurations.put(name, configuration);
         }
         log.tracef("Loaded configurations from local persistence: %s", configurations.keySet());
         return configurations;
      } catch (FileNotFoundException e) {
         // Ignore
         return Collections.emptyMap();
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private void storeAll() {
      // Renaming is done using file locks, which are acquired by the entire JVM.
      // Use a regular lock as well, so another thread cannot lock the file at the same time
      // and cause an OverlappingFileLockException.
      persistenceLock.lock();
      try {
         GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         File sharedDirectory = new File(globalConfiguration.globalState().sharedPersistentLocation());
         sharedDirectory.mkdirs();
         File temp = File.createTempFile("caches", null, sharedDirectory);
         Map<String, Configuration> configurationMap = new HashMap<>();
         for (String cacheName : persistentCaches) {
            configurationMap.put(cacheName, configurationManager.getConfiguration(cacheName, true));
         }
         try (FileOutputStream f = new FileOutputStream(temp)) {
            parserRegistry.serialize(f, null, configurationMap);
         }
         File persistentFile = getPersistentFile();
         try {
            renameTempFile(temp, getPersistentFileLock(), persistentFile);
         } catch (Exception e) {
            throw CONFIG.cannotRenamePersistentFile(temp.getAbsolutePath(), persistentFile, e);
         }
      } catch (Exception e) {
         throw CONFIG.errorPersistingGlobalConfiguration(e);
      } finally {
         persistenceLock.unlock();
      }
   }

   private File getPersistentFile() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml");
   }

   private File getPersistentFileLock() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml.lck");
   }
}
