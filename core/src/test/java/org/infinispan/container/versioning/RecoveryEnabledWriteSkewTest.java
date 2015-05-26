package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * It tests the write skew with recovery.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
@Test(groups = "functional", testName = "container.versioning.RecoveryEnabledWriteSkewTest")
public class RecoveryEnabledWriteSkewTest extends AbstractClusteredWriteSkewTest {
   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Override
   protected int clusterSize() {
      return 2;
   }

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.transaction().useSynchronization(false);
      builder.transaction().recovery().enabled(true);
   }
}
