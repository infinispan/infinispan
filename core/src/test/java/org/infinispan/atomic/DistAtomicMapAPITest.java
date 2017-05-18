package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * AtomicMapAPITest with Distributed mode
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = {"functional", "smoke"}, testName = "atomic.DistAtomicMapAPITest")
public class DistAtomicMapAPITest extends AtomicMapAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configurationBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      configurationBuilder.clustering().hash().numOwners(1).groups().enabled();
      createClusteredCaches(2, "atomic", configurationBuilder);
   }

}
