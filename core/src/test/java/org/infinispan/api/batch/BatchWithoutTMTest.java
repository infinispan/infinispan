package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "api.batch.BatchWithoutTMTest")
public class BatchWithoutTMTest extends AbstractBatchTest {

   EmbeddedCacheManager cm;

   @BeforeClass
   public void createCacheManager() {
      final ConfigurationBuilder defaultConfiguration = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      defaultConfiguration.invocationBatching().enable().transaction().autoCommit(false);
      cm = TestCacheManagerFactory.createCacheManager(defaultConfiguration);
   }

   @AfterClass
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testBatchWithoutCfg() {
      Cache<String, String> cache = null;
      cache = createCache(false, "testBatchWithoutCfg");
      try {
         cache.startBatch();
         assert false : "Should have failed";
      }
      catch (CacheConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(true);
         assert false : "Should have failed";
      }
      catch (CacheConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(false);
         assert false : "Should have failed";
      }
      catch (CacheConfigurationException good) {
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


   private static final Log log = LogFactory.getLog(BatchWithoutTMTest.class);

   public void testBatchVisibility() throws InterruptedException {
      Cache<String, String> cache = null;
      cache = createCache(true, "testBatchVisibility");

      log.info("Here it starts...");

      cache.startBatch();
      cache.put("k", "v");
      assertEquals(getOnDifferentThread(cache, "k"), null , "Other thread should not see batch update till batch completes!");

      cache.endBatch(true);

      assertEquals("v", getOnDifferentThread(cache, "k"));
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
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.invocationBatching().enable(enableBatch);
      cm.defineConfiguration(name, c.build());
      return cm.getCache(name);
   }
}
