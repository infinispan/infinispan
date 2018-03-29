package org.infinispan.container.versioning;

import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(testName = "container.versioning.VersionedReplStateTransferTest", groups = "functional")
public class VersionedReplStateTransferTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder builder;

   @Override
   public Object[] factory() {
      return new Object[] {
            new VersionedReplStateTransferTest().totalOrder(false),
            new VersionedReplStateTransferTest().totalOrder(true)
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .transaction().lockingMode(LockingMode.OPTIMISTIC)
            .recovery().disable();

      if (totalOrder) {
         builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      }

      createCluster(builder, 2);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      cache0.put("hello", "world");

      assertEquals("world", cache0.get("hello"));
      assertEquals("world", cache1.get("hello"));

      tm(1).begin();
      assertEquals("world", cache1.get("hello"));
      Transaction t = tm(1).suspend();

      // create a cache2
      addClusterEnabledCacheManager(builder);
      Cache<Object, Object> cache2 = cache(2);

      assertEquals("world", cache2.get("hello"));

      cacheManagers.get(0).stop();
      cacheManagers.remove(0);
      waitForNoRebalance(caches());

      // Cause a write skew
      cache2.put("hello", "new world");

      tm(1).resume(t);
      cache1.put("hello", "world2");

      //write skew should abort the transaction
      expectException(RollbackException.class, tm(1)::commit);

      assertEquals("new world", cache1.get("hello"));
      assertEquals("new world", cache2.get("hello"));
   }
}
