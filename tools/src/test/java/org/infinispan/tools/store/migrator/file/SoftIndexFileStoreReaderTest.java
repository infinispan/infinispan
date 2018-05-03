package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.INDEX_LOCATION;
import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.AbstractReaderTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreType;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.file.SoftIndexFileStoreReaderTest", groups = "functional")
public class SoftIndexFileStoreReaderTest extends AbstractReaderTest {

   private static final String SOURCE_DIR = "target/test-classes/softindex/";
   private static final String TARGET_DATA_DIR = SOURCE_DIR + "/target-softindex/data";
   private static final String TARGET_INDEX_DIR = SOURCE_DIR + "/target-softindex/index";

   @Override
   public Configuration getTargetCacheConfig() {
      return new ConfigurationBuilder().persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .dataLocation(TARGET_DATA_DIR).indexLocation(TARGET_INDEX_DIR)
            .preload(true).ignoreModifications(true)
            .build();
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.SOFT_INDEX_FILE_STORE.toString());
      if (type == SOURCE) {
         properties.put(propKey(type, LOCATION), SOURCE_DIR);
      } else {
         properties.put(propKey(type, LOCATION), TARGET_DATA_DIR);
         properties.put(propKey(type, INDEX_LOCATION), TARGET_INDEX_DIR);
      }
   }
}
