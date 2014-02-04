package org.infinispan.lucene.locking;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * LockManagerFunctionalTest but with Transactional cache..
 *
 * @author Anna Manukyan
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.locking.TransactionalLockManagerUsingBaseLockFactoryTest")
public class TransactionalLockManagerUsingBaseLockFactoryTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration(getTransactionsMode());
      createClusteredCaches(2, "lucene", configurationBuilder);
   }

   protected TransactionMode getTransactionsMode() {
      return TransactionMode.TRANSACTIONAL;
   }
}
