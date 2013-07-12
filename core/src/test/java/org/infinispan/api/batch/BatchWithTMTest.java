package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.testng.Assert.assertEquals;


@Test(groups = {"functional", "transaction"}, testName = "api.batch.BatchWithTMTest")
public class BatchWithTMTest extends AbstractBatchTest {

   EmbeddedCacheManager cm;

   @BeforeClass
   public void createCacheManager() {
      cm = TestCacheManagerFactory.createCacheManager(false);
   }

   @AfterClass
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testBatchWithOngoingTM() throws Exception {
      Cache<String, String> cache = null;
      cache = createCache("testBatchWithOngoingTM");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("k", "v");
      cache.startBatch();
      cache.put("k2", "v2");
      tm.commit();

      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));

      cache.endBatch(false); // should be a no op
      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));
   }

   public void testBatchWithoutOngoingTMSuspension() throws Exception {
      Cache<String, String> cache = createCache("testBatchWithoutOngoingTMSuspension");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm.getTransaction() == null : "Should have no ongoing txs";
      cache.startBatch();
      cache.put("k", "v");
      assert tm.getTransaction() == null : "Should have no ongoing txs";
      cache.put("k2", "v2");

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      try {
         tm.commit(); // should have no effect
      }
      catch (Exception e) {
         // the TM may barf here ... this is OK.
      }

      assert tm.getTransaction() == null : "Should have no ongoing txs";

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(true); // should be a no op

      assert "v".equals(getOnDifferentThread(cache, "k"));
      assert "v2".equals(getOnDifferentThread(cache, "k2"));
   }

   public void testBatchRollback() throws Exception {
      Cache<String, String> cache = createCache("testBatchRollback");
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertEquals(getOnDifferentThread(cache, "k"), null);
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(false);

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;
   }

   private Cache<String, String> createCache(String name) {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      c.invocationBatching().enable();
      cm.defineConfiguration(name, c.build());
      return cm.getCache(name);
   }
}
