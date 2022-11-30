package org.infinispan.tools.store.migrator.file;

import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.nio.file.Path;
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
            new SingleFileStoreReaderTest().majorVersion(9),
            new SingleFileStoreReaderTest().majorVersion(9).targetSegments(59),
            new SingleFileStoreReaderTest().majorVersion(10),
            new SingleFileStoreReaderTest().majorVersion(10).sourceSegments(4),
            new SingleFileStoreReaderTest().majorVersion(11),
            new SingleFileStoreReaderTest().majorVersion(11).sourceSegments(4),
            new SingleFileStoreReaderTest().majorVersion(12),
            new SingleFileStoreReaderTest().majorVersion(12).sourceSegments(4),
            new SingleFileStoreReaderTest().targetSegments(59),
      };
   }

   @Override
   public ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = super.getTargetCacheConfig();
      builder.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class).location(getTargetDir())
            .preload(true).ignoreModifications(true).segmented(targetSegments > 0);
      return builder;
   }

   @Override
   protected void configureStoreProperties(Properties properties, Element type) {
      super.configureStoreProperties(properties, type);
      properties.put(propKey(type, TYPE), StoreType.SINGLE_FILE_STORE.toString());
      properties.put(propKey(type, LOCATION), type == SOURCE ? getSourceDir() : getTargetDir());
   }

   private String getSourceDir() {
      String root = String.format("target/test-classes/infinispan%d/singlefilestore/", majorVersion);
      if (sourceSegments == 0)
         return root;

      return Path.of(root).resolve("segmented").toString();
   }

   private String getTargetDir() {
      return getSourceDir() + "/target-sfs";
   }
}
