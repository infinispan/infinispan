package org.infinispan.tx;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test for: https://jira.jboss.org/jira/browse/ISPN-149.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "tx.LargeTransactionTest", groups = "functional")
public class LargeTransactionTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(LargeTransactionTest.class);

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .remoteTimeout(30000)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .syncCommitPhase(true).syncRollbackPhase(true)
            .invocationBatching().enable()
            .locking().lockAcquisitionTimeout(60000).useLockStriping(false);

      EmbeddedCacheManager container = TestCacheManagerFactory.createClusteredCacheManager(c);
      container.start();
      registerCacheManager(container);
      container.startCaches(CacheContainer.DEFAULT_CACHE_NAME, "TestCache");
      Cache cache1 = container.getCache("TestCache");
      assert cache1.getCacheConfiguration().clustering().cacheMode().equals(CacheMode.REPL_SYNC);
      cache1.start();

      container = TestCacheManagerFactory.createClusteredCacheManager(c);
      container.start();
      registerCacheManager(container);
      container.startCaches(CacheContainer.DEFAULT_CACHE_NAME, "TestCache");
      Cache cache2 = container.getCache("TestCache");
      assert cache2.getCacheConfiguration().clustering().cacheMode().equals(CacheMode.REPL_SYNC);
   }

   public void testLargeTx() throws Exception {
      Cache cache1 = cache(0, "TestCache");
      Cache cache2 = cache(1, "TestCache");
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      for (int i = 0; i < 200; i++)
         cache1.put("key" + i, "value" + i);
      log.trace("___________ before commit");
      tm.commit();

      for (int i = 0; i < 200; i++) {
         assert cache2.get("key" + i).equals("value"+i);
      }
   }

   public void testSinglePutInTx() throws Exception {
      Cache cache1 = cache(0, "TestCache");
      Cache cache2 = cache(1, "TestCache");
      TransactionManager tm = TestingUtil.getTransactionManager(cache1);

      tm.begin();
      cache1.put("key", "val");
      log.trace("___________ before commit");
      tm.commit();

      assert cache2.get("key").equals("val");
   }

   public void testSimplePutNoTx() {
      Cache cache1 = cache(0, "TestCache");
      Cache cache2 = cache(1, "TestCache");
      cache1.put("key", "val");
      assert cache2.get("key").equals("val");
   }
}
