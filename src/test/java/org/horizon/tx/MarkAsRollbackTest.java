package org.horizon.tx;

import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.config.Configuration;
import org.horizon.manager.CacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.MarkAsRollbackTest")
public class MarkAsRollbackTest extends SingleCacheManagerTest {

   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      CacheManager cm = TestCacheManagerFactory.createCacheManager(c);
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
