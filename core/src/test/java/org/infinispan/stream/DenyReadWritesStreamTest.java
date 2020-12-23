package org.infinispan.stream;

import java.util.Map;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test the handling of backpressure when partition handling is enabled.
 *
 * See ISPN-12594.
 *
 * @author Wolf-Dieter Fink
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "stream.DenyReadWritesStreamTest")
public class DenyReadWritesStreamTest extends SingleCacheManagerTest {
   private static final Log log = LogFactory.getLog(DenyReadWritesStreamTest.class);
   public static final int CHUNK_SIZE = 2;
   public static final int NUM_KEYS = 20;

   public DenyReadWritesStreamTest() {

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // Setup up a clustered cache manager
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      cacheManager = new DefaultCacheManager(global.build());
      ConfigurationBuilder builder = new ConfigurationBuilder();

      // With a dist cache
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
             .stateTransfer().chunkSize(CHUNK_SIZE)
             .hash().numOwners(4);

      // DENY_READ_WRITES will cause iteration to fail
      // ALLOW_READS or ALLOW_READ_WRITES work as expected
      builder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);

      cacheManager.defineConfiguration("testCache", builder.build());
      cache = cacheManager.getCache("testCache");

      for (int i = 0; i < NUM_KEYS; i++) {
         cache.put(String.valueOf(i), String.valueOf(i));
      }

      return cacheManager;
   }

   @Override
   protected void clearCacheManager() {
      // Do nothing
   }

   public void testValuesForEachNoBatchSize() {
      try (CacheStream<Object> cacheStream = cache.values().stream()) {
         cacheStream.forEach(v -> {
            log.tracef("foreach: %s", v);
         });
      }
   }

   public void testEntriesIteratorNoBatchSize() {
      try (CloseableIterator<Map.Entry<Object, Object>> it = cache.entrySet().iterator()) {
         while (it.hasNext()) {
            Object key = it.next();
            log.tracef("iterator: %s", key);
         }
      }
   }

   public void testKeysForEachBatchSizeEqualsCacheSize() {
      try (CacheStream<Object> cacheStream = cache.keySet().stream().distributedBatchSize(NUM_KEYS)) {
         cacheStream.forEach(k -> {
            log.tracef("foreach: %s", k);
         });
      }
   }

   public void testKeysForEachBatchSizeIsLessThanCacheSize() {
      try (CacheStream<Object> cacheStream = cache.keySet().stream().distributedBatchSize(NUM_KEYS - 2)) {
         cacheStream.forEach(k -> {
            log.tracef("foreach %s", k);
         });
      }
   }
}
