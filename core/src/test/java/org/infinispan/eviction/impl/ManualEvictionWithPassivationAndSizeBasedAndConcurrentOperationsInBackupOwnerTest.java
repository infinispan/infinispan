package org.infinispan.eviction.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests manual eviction with concurrent read and/or write operation. This test has passivation enabled and the eviction
 * happens in the backup owner
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInBackupOwnerTest")
public class ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInBackupOwnerTest
      extends ManualEvictionWithSizeBasedAndConcurrentOperationsInBackupOwnerTest {
   {
      passivation = true;
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }
}
