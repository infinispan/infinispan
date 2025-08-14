package org.infinispan.persistence.support;

import java.util.HashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
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
   private String tmpDirectory;

   public BatchAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.globalState().enable().persistentLocation(tmpDirectory);

      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.invocationBatching().enable();
      enableTestJdbcStorage(configuration);

      return TestCacheManagerFactory.createCacheManager(globalBuilder, configuration);
   }

   private void enableTestJdbcStorage(ConfigurationBuilder configuration) throws Exception {
      configuration
         .persistence()
            .addSoftIndexFileStore()
               .async()
                  .enable();
   }

   @Test
   public void sequentialOverwritingInBatches() {
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

   @Test(dependsOnMethods = "sequentialOverwritingInBatches")
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

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }
}
