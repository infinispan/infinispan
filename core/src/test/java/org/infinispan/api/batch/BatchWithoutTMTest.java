package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.batch.BatchWithoutTMTest")
public class BatchWithoutTMTest extends AbstractBatchTest {

   CacheManager cm;

   @BeforeClass
   public void createCacheManager() {
      cm = TestCacheManagerFactory.createLocalCacheManager();
   }

   @AfterClass
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testBatchWithoutCfg() {
      Cache<String, String> cache = null;
      cache = createCache(false, "testBatchWithoutCfg");
      try {
         cache.startBatch();
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(true);
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(false);
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }
   }

   public void testEndBatchWithoutStartBatch() {
      Cache<String, String> cache = null;
      cache = createCache(true, "testEndBatchWithoutStartBatch");
      cache.endBatch(true);
      cache.endBatch(false);
      // should not fail.
   }

   public void testStartBatchIdempotency() {
      Cache<String, String> cache = null;
      cache = createCache(true, "testStartBatchIdempotency");
      cache.startBatch();
      cache.put("k", "v");
      cache.startBatch();     // again
      cache.put("k2", "v2");
      cache.endBatch(true);

      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));
   }

   public void testBatchVisibility() throws InterruptedException {
      Cache<String, String> cache = null;
      cache = createCache(true, "testBatchVisibility");
      cache.startBatch();
      cache.put("k", "v");
      assert getOnDifferentThread(cache, "k") == null : "Other thread should not see batch update till batch completes!";

      cache.endBatch(true);

      assert "v".equals(getOnDifferentThread(cache, "k"));
   }

   public void testBatchRollback() throws Exception {
      Cache<String, String> cache = null;
      cache = createCache(true, "testBatchRollback");
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(false);

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;
   }

   private Cache<String, String> createCache(boolean enableBatch, String name) {
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(enableBatch);
      cm.defineCache(name, c);
      return cm.getCache(name);
   }
}
