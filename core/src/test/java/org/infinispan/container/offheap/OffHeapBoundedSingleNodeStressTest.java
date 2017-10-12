package org.infinispan.container.offheap;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for native memory fragmentation when the values have different size.
 *
 * @author Dan Berindei
 */
@Test(groups = "stress", testName = "container.offheap.OffHeapBoundedSingleNodeStressTest")
public class OffHeapBoundedSingleNodeStressTest extends OffHeapMultiNodeStressTest {

   @Override protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dcc.memory().storageType(StorageType.OFF_HEAP).evictionType(EvictionType.COUNT).size(2000);
      // Only start up the 1 cache
      addClusterEnabledCacheManager(dcc);
   }

   public void testLotsOfWrites() throws Exception {
      final int WRITE_THREADS = 5;
      final int INSERTIONCOUNT = 1_000_000;
      final int KEY_SIZE = 30;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS, getTestThreadFactory("Worker"));
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(execService);

      try {
         final Map<byte[], byte[]> map = cache(0);

         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  byte[] key = randomBytes(KEY_SIZE);
                  byte[] value = randomBytes(j / 100);
                  map.put(key, value);
                  if (j % 1000 == 0) {
                     log.debugf("%d entries written", j);
                  }
               }
               return null;
            });
         }

         for (int i = 0; i < WRITE_THREADS; ++i) {
            Future<Void> future = service.poll(1000, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }
      } finally {
         execService.shutdown();
         execService.awaitTermination(100, TimeUnit.SECONDS);
      }
   }

}
