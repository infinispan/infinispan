package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test modifications to an AtomicMap during state transfer are consistent across
 * a distributed cluster.
 *
 * @author Ryan Emerson
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "atomic.DistAtomicMapStateTransferTest")
public class DistAtomicMapStateTransferTest extends BaseAtomicMapStateTransferTest {

   public DistAtomicMapStateTransferTest() {
      super(CacheMode.DIST_SYNC, TransactionMode.TRANSACTIONAL);
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder configurationBuilder = super.getConfigurationBuilder();
      configurationBuilder.clustering().hash().numOwners(1);
      return configurationBuilder;
   }
}
