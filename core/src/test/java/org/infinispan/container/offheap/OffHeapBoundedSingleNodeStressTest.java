package org.infinispan.container.offheap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.testng.annotations.Test;

/**
 * Test for native memory fragmentation when the values have different size.
 *
 * @author Dan Berindei
 * @author William Burns
 */
@Test(groups = "stress", testName = "container.offheap.OffHeapBoundedSingleNodeStressTest")
public class OffHeapBoundedSingleNodeStressTest extends OffHeapMultiNodeStressTest {

   @Override protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dcc.memory().storage(StorageType.OFF_HEAP).maxCount(500);
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
         execService.awaitTermination(10, TimeUnit.SECONDS);
      }
   }


   public void testLotsOfPutsAndReadsIntoDataContainer() throws InterruptedException, ExecutionException {
      int WRITE_THREADS = 4;
      int READ_THREADS = 16;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + READ_THREADS,
            getTestThreadFactory("Worker"));
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(execService);

      try {
         Cache<WrappedBytes, WrappedBytes> cache = cache(0);
         final DataContainer<WrappedBytes, WrappedBytes> map = cache.getAdvancedCache().getDataContainer();

         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               KeyGenerator generator = new KeyGenerator();
               while (!Thread.interrupted()) {
                  WrappedByteArray key = generator.getNextKey();
                  WrappedByteArray value = generator.getNextValue();
                  map.put(key, value, generator.getMetadata());
               }
               return null;
            });
         }

         for (int i = 0; i < READ_THREADS; ++i) {
            service.submit(() -> {
               KeyGenerator generator = new KeyGenerator();
               while (!Thread.interrupted()) {
                  WrappedByteArray key = generator.getNextKey();
                  InternalCacheEntry<WrappedBytes, WrappedBytes> innerV = map.peek(key);
                  // Here just to make sure get doesn't get optimized away
                  if (innerV != null && innerV.equals(cache)) {
                     System.out.println(System.currentTimeMillis());
                  }
               }
               return null;
            });
         }

         // This is how long this test will run for
         Future<Void> future = service.poll(30, TimeUnit.SECONDS);
         if (future != null) {
            future.get();
         }
      } finally {
         execService.shutdownNow();
         execService.awaitTermination(10, TimeUnit.SECONDS);
      }
   }
}
