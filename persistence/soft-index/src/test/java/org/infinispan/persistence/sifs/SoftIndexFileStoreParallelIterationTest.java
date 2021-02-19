package org.infinispan.persistence.sifs;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreParallelIterationTest")
public class SoftIndexFileStoreParallelIterationTest extends ParallelIterationTest {

   protected String location;

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      location = CommonsTestingUtil.tmpDirectory(this.getClass());
      cb.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .segmented(true)
            .dataLocation(location + "/data")
            .indexLocation(location);
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(location);
   }

   @Override
   protected boolean hasMetadata(boolean fetchValues, int i) {
      // We only include metadata if the value is requested
      return fetchValues && super.insertMetadata(i);
   }
}
