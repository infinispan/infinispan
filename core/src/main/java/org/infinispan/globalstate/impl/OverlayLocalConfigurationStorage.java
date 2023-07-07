
package org.infinispan.globalstate.impl;

import static org.infinispan.commons.util.Util.renameTempFile;
import static org.infinispan.util.logging.Log.CONFIG;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Set<String> persistentCaches = ConcurrentHashMap.newKeySet();
   private final Set<String> persistentTemplates = ConcurrentHashMap.newKeySet();

   private final Lock persistenceLock = new ReentrantLock();

   @Override
   public void validateFlags(EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
      if (!flags.contains(CacheContainerAdmin.AdminFlag.VOLATILE) && !globalConfiguration.globalState().enabled())
         throw CONFIG.globalStateDisabled();
   }

   @Override
   public CompletionStage<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletionStage<Void> future = super.createTemplate(name, configuration, flags);
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
   public CompletionStage<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return blockingManager.<Void>supplyBlocking(() -> {
         if (persistentTemplates.remove(name)) {
            storeTemplates();
         }
         removeTemplateSync(name, flags);
         return null;
      }, name).toCompletableFuture();
   }

   @Override
   public CompletionStage<Void> createCache(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletionStage<Void> future = super.createCache(name, template, configuration, flags);
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

   @Override
   public CompletionStage<Void> updateConfiguration(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      CompletionStage<Void> future = super.updateConfiguration(name, configuration, flags);
      if (persistentCaches.contains(name)) {
         return blockingManager.thenApplyBlocking(future, (v) -> {
            storeCaches();
            return v;
         }, name).toCompletableFuture();
      } else {
         return future;
      }
   }

   @Override
   public CompletionStage<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
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
         ConfigurationBuilderHolder holder = configurationManager.toBuilderHolder();
         parserRegistry.parse(fis, holder, new URLConfigurationResourceResolver(file.toURI().toURL()), MediaType.APPLICATION_XML);
         Collection<String> definedConfigurations = configurationManager.getDefinedConfigurations();
         for (Map.Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            String name = entry.getKey();
            if (!definedConfigurations.contains(name)) {
               Configuration configuration = entry.getValue().build();
               configurations.put(name, configuration);
            }
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
      // Renaming is done using file locks, which are acquired by the entire JVM.
      // Use a regular lock as well, so another thread cannot lock the file at the same time
      // and cause an OverlappingFileLockException.
      persistenceLock.lock();
      try {
         GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         File sharedDirectory = new File(globalConfiguration.globalState().sharedPersistentLocation());
         sharedDirectory.mkdirs();
         File temp = File.createTempFile(prefix, null, sharedDirectory);
         Map<String, Configuration> configurationMap = new HashMap<>();
         for (String config : configNames) {
            Configuration configuration = configurationManager.getConfiguration(config, true);
            if (configuration == null) {
               log.configurationNotFound(config, configurationManager.getDefinedConfigurations());
            } else {
               configurationMap.put(config, configuration);
            }
         }
         try (ConfigurationWriter writer = ConfigurationWriter.to(new FileOutputStream(temp)).clearTextSecrets(true).prettyPrint(true).build()) {
            parserRegistry.serialize(writer, null, configurationMap);
         }
         try {
            renameTempFile(temp, lock, file);
         } catch (Exception e) {
            throw CONFIG.cannotRenamePersistentFile(temp.getAbsolutePath(), file, e);
         }
      } catch (Exception e) {
         throw CONFIG.errorPersistingGlobalConfiguration(e);
      } finally {
         persistenceLock.unlock();
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
