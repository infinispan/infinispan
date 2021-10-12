package org.infinispan.persistence;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.metadata.Metadata;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @since 13.0
 */
@Test(groups = "functional", testName = "persistence.SoftIndexFileStoreParallelIterationTest")
public class SoftIndexFileStoreParallelIterationTest extends ParallelIterationTest {

   protected String dataLocation;
   protected String indexLocation;

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      dataLocation = CommonsTestingUtil.tmpDirectory(this.getClass().getSimpleName(), "data");
      indexLocation = CommonsTestingUtil.tmpDirectory(this.getClass().getSimpleName(), "index");
      cb.persistence().addSoftIndexFileStore()
            .dataLocation(dataLocation)
            .indexLocation(indexLocation);
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(dataLocation);
      Util.recursiveFileRemove(indexLocation);
   }

   @Override
   protected boolean hasMetadata(boolean fetchValues, int i) {
      return fetchValues && super.hasMetadata(fetchValues, i);
   }

   @Override
   protected void assertMetadataEmpty(Metadata metadata, Object key) {
      // SoftIndexFileStore may return metadata even when not requested as it may already be in memory and there is
      // no real reason to skip it
   }
}
