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

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
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
   private Set<String> persistentTemplates = ConcurrentHashMap.newKeySet();

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE) && !globalConfiguration.globalState().enabled())
         throw CONFIG.globalStateDisabled();
   }

   @Override
   public CompletableFuture<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletableFuture<Void> future = super.createTemplate(name, configuration, flags);
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE)) {
         return blockingManager.thenApplyBlocking(future, (v) -> {
            persistentTemplates.add(name);
            storeTemplates();
            return v;
         }, name).toCompletableFuture();
      } else {
         return future;
      }
   }

   @Override
   public CompletableFuture<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return blockingManager.<Void>supplyBlocking(() -> {
         if (persistentTemplates.remove(name)) {
            storeTemplates();
         }
         removeTemplateSync(name, flags);
         return null;
      }, name).toCompletableFuture();
   }

   public CompletableFuture<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletableFuture<Void> future = super.createCache(name, template, configuration, flags);
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE)) {
         return blockingManager.thenApplyBlocking(future, (v) -> {
            persistentCaches.add(name);
            storeCaches();
            return v;
         }, name).toCompletableFuture();
      } else {
         return future;
      }
   }

   public CompletableFuture<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return blockingManager.<Void>supplyBlocking(() -> {
         if (persistentCaches.remove(name)) {
            storeCaches();
         }
         removeCacheSync(name, flags);
         return null;
      }, name).toCompletableFuture();
   }

   @Override
   public Map<String, Configuration> loadAllCaches() {
      Map<String, Configuration> configs = load(getCachesFile());
      log.tracef("Loaded cache configurations from local persistence: %s", configs.keySet());
      return configs;
   }

   @Override
   public Map<String, Configuration> loadAllTemplates() {
      Map<String, Configuration> configs = load(getTemplateFile());
      log.tracef("Loaded templates from local persistence: %s", configs.keySet());
      return configs;
   }

   private Map<String, Configuration> load(File file) {
      try (FileInputStream fis = new FileInputStream(file)) {
         Map<String, Configuration> configurations = new HashMap<>();
         ConfigurationBuilderHolder holder = parserRegistry.parse(fis, null, MediaType.APPLICATION_XML);
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

   private void storeCaches() {
      persistConfigurations("caches", getCachesFile(), getCachesFileLock(), persistentCaches);
   }

   private void storeTemplates() {
      persistConfigurations("templates", getTemplateFile(), getTemplateFileLock(), persistentTemplates);
   }

   private void persistConfigurations(String prefix, File file, File lock, Set<String> configNames) {
      try {
         GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         File sharedDirectory = new File(globalConfiguration.globalState().sharedPersistentLocation());
         sharedDirectory.mkdirs();
         File temp = File.createTempFile(prefix, null, sharedDirectory);
         Map<String, Configuration> configurationMap = new HashMap<>();
         for (String config : configNames) {
            configurationMap.put(config, configurationManager.getConfiguration(config, true));
         }
         try (FileOutputStream f = new FileOutputStream(temp)) {
            parserRegistry.serialize(f, null, configurationMap);
         }
         try {
            renameTempFile(temp, lock, file);
         } catch (Exception e) {
            throw CONFIG.cannotRenamePersistentFile(temp.getAbsolutePath(), file, e);
         }
      } catch (Exception e) {
         throw CONFIG.errorPersistingGlobalConfiguration(e);
      }
   }

   private File getCachesFile() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml");
   }

   private File getCachesFileLock() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "caches.xml.lck");
   }

   private File getTemplateFile() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "templates.xml");
   }

   private File getTemplateFileLock() {
      return new File(configurationManager.getGlobalConfiguration().globalState().sharedPersistentLocation(), "templates.xml.lck");
   }
}
