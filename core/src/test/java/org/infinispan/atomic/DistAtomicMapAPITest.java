package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * AtomicMapAPITest with Distributed mode
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.DistAtomicMapAPITest")
public class DistAtomicMapAPITest extends AtomicMapAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configurationBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .syncCommitPhase(true)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(2000l);
      configurationBuilder.clustering().hash().numOwners(1);
      createClusteredCaches(2, "atomic", configurationBuilder);
   }

}
