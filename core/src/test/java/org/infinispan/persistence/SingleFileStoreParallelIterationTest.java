package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.test.TestingUtil;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class SingleFileStoreParallelIterationTest extends ParallelIterationTest {

   protected String location;

   protected void configurePersistence(ConfigurationBuilder cb) {
      location = TestingUtil.tmpDirectory(this);
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
