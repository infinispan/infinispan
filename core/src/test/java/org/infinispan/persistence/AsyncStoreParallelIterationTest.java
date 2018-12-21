package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.0
 */
@Test(groups = "functional", testName = "persistence.AsyncStoreParallelIterationTest")
@CleanupAfterMethod
public class AsyncStoreParallelIterationTest extends ParallelIterationTest {

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      DummyInMemoryStoreConfigurationBuilder discb =
            cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      discb.async().enabled(true);
   }

   @Override
   protected void teardown() {
      super.teardown();
   }

   @Override
   protected void assertMetadataEmpty(Metadata metadata) {
      // Async store always returns the metadata
   }
}
