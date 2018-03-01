package org.infinispan.persistence.sifs;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreParallelIterationTest")
public class SoftIndexFileStoreParallelIterationTest extends ParallelIterationTest {

   protected String location;

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      location = TestingUtil.tmpDirectory(this.getClass());
      cb.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .dataLocation(location + "/data")
            .indexLocation(location);
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(location);
   }

}
