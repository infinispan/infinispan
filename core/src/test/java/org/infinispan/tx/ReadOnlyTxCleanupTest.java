package org.infinispan.tx;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

@Test(testName = "tx.ReadOnlyTxCleanupTest", groups = "functional")
@CleanupAfterMethod
public class ReadOnlyTxCleanupTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testReadOnlyTx() throws SystemException, RollbackException, HeuristicRollbackException, HeuristicMixedException, NotSupportedException {
      Cache<String, String> c1 = cacheManager.getCache();
      cacheManager.defineConfiguration("two", cacheManager.getDefaultCacheConfiguration());
      Cache<String, String> c2 = cacheManager.getCache("two");

      c1.put("c1", "c1");
      c2.put("c2", "c2");

      TransactionManager tm1 = tm();
      tm1.begin();
      c1.get("c1");
      c2.get("c2");
      tm1.commit();

      TransactionTable tt1 = TestingUtil.extractComponent(c1, TransactionTable.class);
      TransactionTable tt2 = TestingUtil.extractComponent(c2, TransactionTable.class);

      assert tt1.getLocalTxCount() == 0;
      assert tt2.getLocalTxCount() == 0;
   }

}
