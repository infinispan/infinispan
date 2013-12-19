package org.infinispan.eviction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInBackupOwnerTest} but with an
 * unbounded data container
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithPassivationAndConcurrentOperationsInBackupOwnerTest", singleThreaded = true)
public class ManualEvictionWithPassivationAndConcurrentOperationsInBackupOwnerTest
      extends ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInBackupOwnerTest {

   @Override
   protected void configureEviction(ConfigurationBuilder builder) {
      builder.eviction().maxEntries(-1).strategy(EvictionStrategy.NONE);
   }
}
