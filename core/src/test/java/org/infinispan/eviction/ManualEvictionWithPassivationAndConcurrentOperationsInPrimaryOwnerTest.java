package org.infinispan.eviction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInPrimaryOwnerTest} but with an
 * unbounded data container
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithPassivationAndConcurrentOperationsInPrimaryOwnerTest")
public class ManualEvictionWithPassivationAndConcurrentOperationsInPrimaryOwnerTest
      extends ManualEvictionWithPassivationAndSizeBasedAndConcurrentOperationsInPrimaryOwnerTest {

   @Override
   protected void configureEviction(ConfigurationBuilder builder) {
      builder.eviction().maxEntries(-1).strategy(EvictionStrategy.NONE);
   }
}
