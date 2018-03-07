package org.infinispan.counter.impl.manager;

import static org.infinispan.commons.util.Util.renameTempFile;
import static org.infinispan.counter.configuration.ConvertUtil.parsedConfigToConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.CounterConfigurationParser;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.counter.logging.Log;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

/**
 * A persistent implementation of {@link CounterConfigurationStorage}.
 * <p>
 * The counter's configuration will be stored (as xml format) in {@code counters.xml} file in {@link GlobalStateConfiguration#sharedPersistentLocation()}
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class PersistedCounterConfigurationStorage implements CounterConfigurationStorage {

   private static final Log log = LogFactory.getLog(PersistedCounterConfigurationStorage.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<String, CounterConfiguration> storage;
   private final CounterConfigurationSerializer serializer;
   private final CounterConfigurationParser parser;
   private volatile String sharedDirectory;

   PersistedCounterConfigurationStorage() {
      storage = new ConcurrentHashMap<>();
      serializer = new CounterConfigurationSerializer();
      parser = new CounterConfigurationParser();
   }

   private static AbstractCounterConfiguration fromEntry(Map.Entry<String, CounterConfiguration> entry) {
      return ConvertUtil.configToParsedConfig(entry.getKey(), entry.getValue());
   }

   private static void renameFailed(File tmpFile, File dstFile) {
      throw log.cannotRenamePersistentFile(tmpFile.getAbsolutePath(), dstFile);
   }

   @Override
   public Map<String, CounterConfiguration> loadAll() {
      try {
         doLoadAll();
      } catch (IOException | XMLStreamException e) {
         throw log.errorReadingCountersConfiguration(e);
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
      } catch (IOException | XMLStreamException e) {
         throw log.errorPersistingCountersConfiguration(e);
      }
   }

   @Override
   public void validatePersistence(CounterConfiguration configuration) {
      //nothing to validate
   }

   @Override
   public void initialize(EmbeddedCacheManager cacheManager) {
      sharedDirectory = cacheManager.getCacheManagerConfiguration().globalState().sharedPersistentLocation();
   }

   private void doStoreAll() throws IOException, XMLStreamException {
      File directory = getSharedDirectory();
      File temp = File.createTempFile("counters", null, directory);
      try (FileOutputStream f = new FileOutputStream(temp)) {
         serializer.serializeConfigurations(f, convertToList());
      }
      renameTempFile(temp, getFileLock(), getPersistentFile(), PersistedCounterConfigurationStorage::renameFailed);
   }

   private void doLoadAll() throws XMLStreamException, IOException {
      try (FileInputStream fis = new FileInputStream(getPersistentFile())) {
         convertToMap(parser.parseConfigurations(fis));
      } catch (FileNotFoundException e) {
         // Ignore
      }
   }

   private List<AbstractCounterConfiguration> convertToList() {
      return storage.entrySet().stream()
            .map(PersistedCounterConfigurationStorage::fromEntry)
            .collect(Collectors.toList());
   }

   private void convertToMap(List<AbstractCounterConfiguration> configs) {
      configs.forEach(c -> storage.put(c.name(), parsedConfigToConfig(c)));
   }

   private File getSharedDirectory() {
      File directory = new File(sharedDirectory);
      if (!directory.exists()) {
         boolean created = directory.mkdirs();
         if (trace) {
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
