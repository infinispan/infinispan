package org.infinispan.tools.store.migrator.rocksdb;

import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.AbstractReaderTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreType;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.rocksdb.RocksDBReaderTest", groups = "functional")
public class RocksDBReaderTest extends AbstractReaderTest {

   private static final String SOURCE_DIR = "target/test-classes/leveldbstore/";
   private static final String TARGET_DIR = SOURCE_DIR + "/rocksdbstore/";

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RocksDBReaderTest().segmented(29),
            new RocksDBReaderTest(),
      };
   }

   public ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = super.getTargetCacheConfig();
      builder.persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class).location(TARGET_DIR).expiredLocation(TARGET_DIR + "-expired-")
            .preload(true).ignoreModifications(true).segmented(segmentCount > 0);
      return builder;
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.ROCKSDB.toString());
      properties.put(propKey(type, LOCATION), type == SOURCE ? SOURCE_DIR : TARGET_DIR);
   }
}
