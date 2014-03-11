package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * FineGrainedAtomicMapAPITest with Distributed mode
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "atomic.DistFineGrainedAtomicMapAPITest")
public class DistFineGrainedAtomicMapAPITest extends FineGrainedAtomicMapAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configurationBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .syncCommitPhase(true)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(100l);
      configurationBuilder.clustering().hash().numOwners(1);
      createClusteredCaches(2, "atomic", configurationBuilder);
   }

}
