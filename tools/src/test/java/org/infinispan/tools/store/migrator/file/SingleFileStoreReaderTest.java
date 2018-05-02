package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.AbstractReaderTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreType;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.file.SingleFileStoreReaderTest", groups = "functional")
public class SingleFileStoreReaderTest extends AbstractReaderTest {

   private static final String SOURCE_DIR = "target/test-classes/singlefilestore/";
   private static final String TARGET_DIR = SOURCE_DIR + "/target-sfs/";

   @Override
   public Configuration getTargetCacheConfig() {
      return new ConfigurationBuilder().persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class).location(TARGET_DIR)
            .preload(true).ignoreModifications(true)
            .build();
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.SINGLE_FILE_STORE.toString());
      properties.put(propKey(type, LOCATION), type == SOURCE ? SOURCE_DIR : TARGET_DIR);
   }
}
