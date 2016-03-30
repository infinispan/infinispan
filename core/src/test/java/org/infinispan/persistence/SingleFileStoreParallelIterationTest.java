package org.infinispan.persistence;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.SingleFileStoreParallelIterationTest")
public class SingleFileStoreParallelIterationTest extends ParallelIterationTest {

   protected String location;

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      location = TestingUtil.tmpDirectory(this.getClass());
      cb.persistence().addStore(SingleFileStoreConfigurationBuilder.class).location(location);
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(location);
   }

}
