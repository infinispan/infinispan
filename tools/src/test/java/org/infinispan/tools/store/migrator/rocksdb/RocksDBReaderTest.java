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

   private String getSourceDir() {
      return String.format("target/test-classes/infinispan%d/leveldbstore/", majorVersion);
   }

   private String getTargetDirectory() {
      return String.format("%s/target/%d/", getSourceDir(), segmentCount);
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RocksDBReaderTest(),
            new RocksDBReaderTest().segmented(59),
            new RocksDBReaderTest().majorVersion(9),
            new RocksDBReaderTest().majorVersion(9).segmented(59),
      };
   }

   public ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = super.getTargetCacheConfig();
      String targetDir = getTargetDirectory();
      builder.persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class).location(targetDir).expiredLocation(targetDir + "expired")
            .preload(true).ignoreModifications(true).segmented(segmentCount > 0);
      return builder;
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.ROCKSDB.toString());
      properties.put(propKey(type, LOCATION), type == SOURCE ? getSourceDir() : getTargetDirectory());
   }
}
