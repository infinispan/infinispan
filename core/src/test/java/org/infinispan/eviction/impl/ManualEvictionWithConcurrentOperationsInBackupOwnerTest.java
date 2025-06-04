package org.infinispan.eviction.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link ManualEvictionWithSizeBasedAndConcurrentOperationsInBackupOwnerTest} but with an unbounded data
 * container
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithConcurrentOperationsInBackupOwnerTest", singleThreaded = true)
public class ManualEvictionWithConcurrentOperationsInBackupOwnerTest
      extends ManualEvictionWithSizeBasedAndConcurrentOperationsInBackupOwnerTest {

   @Override
   protected void configureEviction(ConfigurationBuilder builder) {
      builder.memory().maxCount(-1);
   }

   @Override
   public void testEvictionDuringRemove() {
      // Ignore this test as it requires size eviction
   }

   @Override
   public void testEvictionDuringWrite() {
      // Ignore this test as it requires size eviction
   }
}
