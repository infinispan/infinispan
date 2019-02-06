package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.INDEX_LOCATION;
import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.AbstractReaderTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreType;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.file.SoftIndexFileStoreReaderTest", groups = "functional")
public class SoftIndexFileStoreReaderTest extends AbstractReaderTest {

   public String getSrcDirectory() {
      return String.format("target/test-classes/infinispan%d/softindex/", majorVersion);
   }

   public String getTargetDataDirectory() {
      return String.format("%s/target-softindex/data/%d/", getSrcDirectory(), segmentCount);
   }

   public String getTargetIndexDirectory() {
      return String.format("%s/target-softindex/index/%d/", getSrcDirectory(), segmentCount);
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new SoftIndexFileStoreReaderTest(),
            new SoftIndexFileStoreReaderTest().segmented(59),
            new SoftIndexFileStoreReaderTest().majorVersion(9),
            new SoftIndexFileStoreReaderTest().majorVersion(9).segmented(59),
      };
   }

   @Override
   public ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = super.getTargetCacheConfig();
      builder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            // Have to append additional name for segment, since SoftIndex creates files named by just numbers
            // so if write to the same location with segmented vs not segmented, they will clash
            .dataLocation(getTargetDataDirectory())
            .indexLocation(getTargetIndexDirectory())
            .preload(true).ignoreModifications(true).segmented(segmentCount > 0);
      return builder;
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.SOFT_INDEX_FILE_STORE.toString());
      if (type == SOURCE) {
         properties.put(propKey(type, LOCATION), getSrcDirectory());
      } else {
         properties.put(propKey(type, LOCATION), getTargetDataDirectory());
         properties.put(propKey(type, INDEX_LOCATION), getTargetIndexDirectory());
      }
   }
}
