package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
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
      TestingUtil.recursiveFileRemove(location);
   }

   @Override
   protected int numThreads() {
      return KnownComponentNames.getDefaultThreads(KnownComponentNames.PERSISTENCE_EXECUTOR) + 1 /** caller's thread */;
   }

}
