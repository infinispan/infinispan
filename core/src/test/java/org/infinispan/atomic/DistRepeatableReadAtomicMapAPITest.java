package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * AtomicMapAPITest with Repeatable Read and Distributed mode
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.DistRepeatableReadAtomicMapAPITest")
public class DistRepeatableReadAtomicMapAPITest extends RepeatableReadAtomicMapAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .syncCommitPhase(true)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .locking().lockAcquisitionTimeout(2000l);
      c.clustering().hash().numOwners(1);
      createClusteredCaches(2, "atomic", c);
   }
}
