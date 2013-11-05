package org.infinispan.persistence.support;

import java.io.File;
import java.util.HashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * BatchAsyncStoreTest performs some additional tests on the AsyncCacheWriter
 * but using batches.
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "persistence.decorators.BatchAsyncStoreTest")
public class BatchAsyncStoreTest extends SingleCacheManagerTest {

   private final HashMap<Object, Object> cacheCopy = new HashMap<Object, Object>();

   public BatchAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.invocationBatching().enable();
      enableTestJdbcStorage(configuration);

      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   private void enableTestJdbcStorage(ConfigurationBuilder configuration) throws Exception {
      configuration
         .persistence()
            .passivation(false)
            .addSingleFileStore()
               .preload(false)
               .shared(true)
               .location(tmpDirectory)
               .async()
                  .enable()
                  .threadPoolSize(1);
   }

   @Test
   public void sequantialOvewritingInBatches() {
      cache = cacheManager.getCache();
      AdvancedCache<Object,Object> advancedCache = cache.getAdvancedCache();
      for (int i = 0; i < 2000;) {
         advancedCache.startBatch();
         putAValue(advancedCache, i++);
         putAValue(advancedCache, i++);
         advancedCache.endBatch(true);
      }
      cacheCopy.putAll(cache);
      cache.stop();
      cacheManager.stop();
   }

   private void putAValue(AdvancedCache<Object, Object> advancedCache, int i) {
      String key = "k" + (i % 13);
      String value = "V" + i;
      advancedCache.put(key, value);
   }

   @Test(dependsOnMethods = "sequantialOvewritingInBatches")
   public void indexWasStored() {
      cache = cacheManager.getCache();
      Assert.assertEquals(0, cache.getAdvancedCache().getDataContainer().size());
      boolean failed = false;
      for (Object key : cacheCopy.keySet()) {
         Object expected = cacheCopy.get(key);
         Object actual = cache.get(key);
         if (!expected.equals(actual)) {
            log.errorf("Failure on key '%s' expected value: '%s' actual value: '%s'", key.toString(), expected, actual);
            failed = true;
         }
      }
      Assert.assertFalse(failed);
      Assert.assertEquals(cacheCopy.keySet().size(), cache.keySet().size(), "have a different number of keys");
   }

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      new File(tmpDirectory).mkdirs();
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }
}
