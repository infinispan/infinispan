package org.horizon.tx;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.manager.CacheManager;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;

@Test(groups = "functional", sequential = true, testName = "tx.TransactionsSpanningReplicatedCaches")
public class TransactionsSpanningReplicatedCaches extends MultipleCacheManagersTest {

   CacheManager cm1, cm2;

   public TransactionsSpanningReplicatedCaches() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Exception {
      cm1 = addClusterEnabledCacheManager();
      cm2 = addClusterEnabledCacheManager();

      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());

      defineCacheOnAllManagers("c1", c);
      defineCacheOnAllManagers("c2", c);
   }

   public void testCommitSpanningCaches() throws Exception {
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");
      Cache c2 = cm1.getCache("c2");
      Cache c2Replica = cm2.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();
      assert c1Replica.isEmpty();
      assert c2Replica.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      for (Cache c : Arrays.asList(c1, c1Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c1key").equals("c1value");
      }

      for (Cache c : Arrays.asList(c2, c2Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c2key").equals("c2value");
      }

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value_new");
      assert c2Replica.get("c2key").equals("c2value");

      Transaction tx = tm.suspend();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
      assert c2Replica.get("c2key").equals("c2value");

      tm.resume(tx);
      tm.commit();

      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value_new");
      assert c2.get("c2key").equals("c2value_new");
      assert c2Replica.get("c2key").equals("c2value_new");
   }

   public void testRollbackSpanningCaches() throws Exception {
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");
      Cache c2 = cm1.getCache("c2");
      Cache c2Replica = cm2.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();
      assert c1Replica.isEmpty();
      assert c2Replica.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      for (Cache c : Arrays.asList(c1, c1Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c1key").equals("c1value");
      }

      for (Cache c : Arrays.asList(c2, c2Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c2key").equals("c2value");
      }

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value_new");
      assert c2Replica.get("c2key").equals("c2value");

      Transaction tx = tm.suspend();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
      assert c2Replica.get("c2key").equals("c2value");

      tm.resume(tx);
      tm.rollback();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
      assert c2Replica.get("c2key").equals("c2value");
   }
}