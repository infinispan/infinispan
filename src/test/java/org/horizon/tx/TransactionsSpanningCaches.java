package org.horizon.tx;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", sequential = true, testName = "tx.TransactionsSpanningCaches")
public class TransactionsSpanningCaches extends SingleCacheManagerTest {

   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      CacheManager cm = new DefaultCacheManager(c);
      cm.defineCache("c1", c);
      cm.defineCache("c2", c);
      return cm;
   }

   public void testCommitSpanningCaches() throws Exception {
      Cache c1 = cacheManager.getCache("c1");
      Cache c2 = cacheManager.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assert !c1.isEmpty();
      assert c1.size() == 1;
      assert c1.get("c1key").equals("c1value");

      assert !c2.isEmpty();
      assert c2.size() == 1;
      assert c2.get("c2key").equals("c2value");

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c2.get("c2key").equals("c2value_new");

      Transaction tx = tm.suspend();

      assert c1.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");

      tm.resume(tx);
      tm.commit();

      assert c1.get("c1key").equals("c1value_new");
      assert c2.get("c2key").equals("c2value_new");
   }

   public void testRollbackSpanningCaches() throws Exception {
      Cache c1 = cacheManager.getCache("c1");
      Cache c2 = cacheManager.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assert !c1.isEmpty();
      assert c1.size() == 1;
      assert c1.get("c1key").equals("c1value");

      assert !c2.isEmpty();
      assert c2.size() == 1;
      assert c2.get("c2key").equals("c2value");

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c2.get("c2key").equals("c2value_new");

      Transaction tx = tm.suspend();

      assert c1.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");

      tm.resume(tx);
      tm.rollback();

      assert c1.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
   }
}
