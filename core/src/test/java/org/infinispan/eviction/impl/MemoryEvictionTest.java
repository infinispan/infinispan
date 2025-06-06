package org.infinispan.eviction.impl;

import java.util.Random;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * This test is useful to test how much memory is in use by the data container.  Since the Java GC may not clean up
 * everything on 1 pass, we have to run multiple passes through until we get a number that is relatively stable.
 *
 * @author William Burns
 */
@Test(groups = "profiling", testName = "eviction.MemoryEvictionTest")
public class MemoryEvictionTest extends SingleCacheManagerTest {

   private final long MAX_MEMORY = 400 * 1000 * 1000;
   private final int MATCH_COUNT = 5;

   private static final Log log = LogFactory.getLog(MemoryEvictionTest.class);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
            .memory().storage(StorageType.HEAP).maxSize(MAX_MEMORY)
            .build();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testSimpleSizeEviction() {
      log.debugf("Max memory: %d", MAX_MEMORY);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      printMemoryUsage(dc.size());
      Random random = new Random();
      int matchCount = 0;
      int byteKeySize = 10;
      int byteValueSize = 100;
      long previousMemorySize = 0;
      for (int j = 0; j < 200; ++j) {
         while (matchCount < this.MATCH_COUNT) {
            for (int i = 0; i < 20000; ++i) {
               byte[] keyBytes = new byte[byteKeySize];
               byte[] valueBytes = new byte[byteValueSize];
               random.nextBytes(keyBytes);
               random.nextBytes(valueBytes);
               cache.getAdvancedCache().put(keyBytes, valueBytes);
            }
            long memorySize = printMemoryUsage(dc.size());
            if (memorySize == previousMemorySize) {
               matchCount++;
            }
            previousMemorySize = memorySize;
         }
      }
   }

   // Also returns size
   private long printMemoryUsage(int cacheSize) {
      System.gc();
      System.gc();
      Runtime runtime = Runtime.getRuntime();
      long usedMemory = runtime.totalMemory() - runtime.freeMemory();
      log.debugf("Used memory = %d, cache size = %d", usedMemory, cacheSize);
      return usedMemory;
   }
}
