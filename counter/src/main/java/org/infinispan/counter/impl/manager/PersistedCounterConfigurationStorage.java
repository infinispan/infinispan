package org.infinispan.counter.impl.manager;

import static org.infinispan.commons.util.Util.renameTempFile;
import static org.infinispan.counter.configuration.ConvertUtil.parsedConfigToConfig;
import static org.infinispan.counter.logging.Log.CONTAINER;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.CounterConfigurationParser;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.LogFactory;

/**
 * A persistent implementation of {@link CounterConfigurationStorage}.
 * <p>
 * The counter's configuration will be stored (as xml format) in {@code counters.xml} file in {@link
 * GlobalStateConfiguration#sharedPersistentLocation()}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Scope(Scopes.GLOBAL)
public class PersistedCounterConfigurationStorage implements CounterConfigurationStorage {

   private static final Log log = LogFactory.getLog(PersistedCounterConfigurationStorage.class, Log.class);
   private final Map<String, CounterConfiguration> storage;
   private final CounterConfigurationSerializer serializer;
   private final CounterConfigurationParser parser;
   private final String sharedDirectory;

   public PersistedCounterConfigurationStorage(GlobalConfiguration globalConfiguration) {
      storage = new ConcurrentHashMap<>(32);
      serializer = new CounterConfigurationSerializer();
      parser = new CounterConfigurationParser();
      sharedDirectory = globalConfiguration.globalState().sharedPersistentLocation();
   }

   private static AbstractCounterConfiguration fromEntry(Map.Entry<String, CounterConfiguration> entry) {
      return ConvertUtil.configToParsedConfig(entry.getKey(), entry.getValue());
   }

   @Override
   public Map<String, CounterConfiguration> loadAll() {
      try {
         doLoadAll();
      } catch (IOException e) {
         throw CONTAINER.errorReadingCountersConfiguration(e);
      }
      return storage;
   }

   @Override
   public void store(String name, CounterConfiguration configuration) {
      if (configuration.storage() == Storage.VOLATILE) {
         //don't store volatile counters!
         return;
      }
      storage.put(name, configuration);
      try {
         doStoreAll();
      } catch (IOException e) {
         throw CONTAINER.errorPersistingCountersConfiguration(e);
      }
   }

   @Override
   public void remove(String name) {
      storage.remove(name);
   }

   @Override
   public void validatePersistence(CounterConfiguration configuration) {
      //nothing to validate
   }

   private void doStoreAll() throws IOException {
      File directory = getSharedDirectory();
      File temp = File.createTempFile("counters", null, directory);
      try (FileOutputStream f = new FileOutputStream(temp)) {
         serializer.serializeConfigurations(f, convertToList());
      }
      File persistentFile = getPersistentFile();
      try {
         renameTempFile(temp, getFileLock(), persistentFile);
      } catch (Exception e) {
         throw CONTAINER.cannotRenamePersistentFile(temp.getAbsolutePath(), persistentFile, e);
      }
   }

   private void doLoadAll() throws IOException {
      File file = getPersistentFile();
      try (FileInputStream fis = new FileInputStream(file)) {
         convertToMap(parser.parseConfigurations(fis));
      } catch (FileNotFoundException e) {
         if (log.isTraceEnabled()) {
            log.tracef("File '%s' does not exist. Skip loading.", file.getAbsolutePath());
         }
      }
   }

   private List<AbstractCounterConfiguration> convertToList() {
      return storage.entrySet().stream()
            .map(PersistedCounterConfigurationStorage::fromEntry)
            .collect(Collectors.toList());
   }

   private void convertToMap(Map<String, AbstractCounterConfiguration> configs) {
      configs.forEach((n, c) -> storage.put(n, parsedConfigToConfig(c)));
   }

   private File getSharedDirectory() {
      File directory = new File(sharedDirectory);
      if (!directory.exists()) {
         boolean created = directory.mkdirs();
         if (log.isTraceEnabled()) {
            log.tracef("Shared directory created? %s", created);
         }
      }
      return directory;
   }

   private File getPersistentFile() {
      return new File(sharedDirectory, "counters.xml");
   }

   private File getFileLock() {
      return new File(sharedDirectory, "counters.xml.lck");
   }
}
