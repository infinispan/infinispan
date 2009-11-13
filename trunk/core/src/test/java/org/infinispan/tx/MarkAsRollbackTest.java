package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.MarkAsRollbackTest")
public class MarkAsRollbackTest extends SingleCacheManagerTest {

   protected CacheManager createCacheManager() throws Exception {
      CacheManager cm = TestCacheManagerFactory.createCacheManager(new Configuration(), true);
      cache = cm.getCache();
      return cm;
   }

   public void testMarkAsRollbackAfterMods() throws Exception {

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm != null;
      tm.begin();
      cache.put("k", "v");
      assert cache.get("k").equals("v");
      tm.setRollbackOnly();
      try {
         tm.commit();
         assert false : "Should have rolled back";
      }
      catch (RollbackException expected) {
      }

      assert tm.getTransaction() == null : "There should be no transaction in scope anymore!";
      assert cache.get("k") == null : "Expected a null but was " + cache.get("k");
   }

   public void testMarkAsRollbackBeforeMods() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm != null;
      tm.begin();
      tm.setRollbackOnly();
      try {
         cache.put("k", "v");
         assert false : "Should have throw an illegal state exception";
      } catch (IllegalStateException expected) {

      }
      try {
         tm.commit();
         assert false : "Should have rolled back";
      }
      catch (RollbackException expected) {

      }

      assert tm.getTransaction() == null : "There should be no transaction in scope anymore!";
      assert cache.get("k") == null : "Expected a null but was " + cache.get("k");
   }
}
