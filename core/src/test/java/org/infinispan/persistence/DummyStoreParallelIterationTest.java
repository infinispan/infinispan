package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.DummyStoreParallelIterationTest")
public class DummyStoreParallelIterationTest extends ParallelIterationTest {

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }

   @Override
   protected int numThreads() {
      return 1;
   }
}
