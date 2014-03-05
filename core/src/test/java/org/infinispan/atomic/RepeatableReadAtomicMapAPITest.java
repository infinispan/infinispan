package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "atomic.RepeatableReadAtomicMapAPITest")
public class RepeatableReadAtomicMapAPITest extends AtomicMapAPITest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .locking().lockAcquisitionTimeout(2000l);
      createClusteredCaches(2, "atomic", c);
   }
}
