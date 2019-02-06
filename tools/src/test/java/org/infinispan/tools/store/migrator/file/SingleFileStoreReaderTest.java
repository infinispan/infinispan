package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.AbstractReaderTest;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreType;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.file.SingleFileStoreReaderTest", groups = "functional")
public class SingleFileStoreReaderTest extends AbstractReaderTest {

   @Factory
   public Object[] factory() {
      return new Object[] {
            new SingleFileStoreReaderTest(),
            new SingleFileStoreReaderTest().segmented(59),
            new SingleFileStoreReaderTest().majorVersion(9),
            new SingleFileStoreReaderTest().majorVersion(9).segmented(59),
      };
   }

   @Override
   public ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = super.getTargetCacheConfig();
      builder.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class).location(getTargetDir())
            .preload(true).ignoreModifications(true).segmented(segmentCount > 0);
      return builder;
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.SINGLE_FILE_STORE.toString());
      properties.put(propKey(type, LOCATION), type == SOURCE ? getSourceDir() : getTargetDir());
   }

   private String getSourceDir() {
      return String.format("target/test-classes/infinispan%d/singlefilestore/", majorVersion);
   }

   private String getTargetDir() {
      return getSourceDir() + "/target-sfs";
   }
}
