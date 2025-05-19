package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "container.offheap.OffHeapMultiNodeStressTest", timeOut = 15 * 60 * 1000)
public class OffHeapMultiNodeStressTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      dcc.memory().storageType(StorageType.OFF_HEAP);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   static DataContainer<WrappedByteArray, WrappedByteArray> castDC(Object obj) {
      return (DataContainer<WrappedByteArray, WrappedByteArray>) obj;
   }

   public void testLotsOfWritesAndFewRemoves() throws Exception {
      final int WRITE_THREADS = 5;
      final int REMOVE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int REMOVECOUNT = 1;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + REMOVE_THREADS,
                                                                 getTestThreadFactory("Worker"));
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(execService);

      try {
         final Map<byte[], byte[]> map = cache(0);

         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  byte[] hcc = randomBytes(KEY_SIZE);
                  map.put(hcc, hcc);
               }
               return null;
            });
         }

         for (int i = 0; i < REMOVE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < REMOVECOUNT; ++j) {
                  Iterator<Map.Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
                  while (iterator.hasNext()) {
                     map.remove(iterator.next().getKey());
                  }
               }
               return null;
            });
         }

         for (int i = 0; i < WRITE_THREADS + REMOVE_THREADS; ++i) {
            Future<Void> future = service.poll(10, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }
      } finally {
         execService.shutdown();
         execService.awaitTermination(1000, TimeUnit.SECONDS);
      }
   }

   public void testWritesAndRemovesWithExecutes() throws Exception {
      final int WRITE_THREADS = 5;
      final int REMOVE_THREADS = 2;
      final int EXECUTE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int REMOVECOUNT = 1;
      final int EXECUTECOUNT = 2;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + REMOVE_THREADS + EXECUTE_THREADS,
                                                                 getTestThreadFactory("Worker"));
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(execService);

      try {
         final Cache<byte[], byte[]> bchm = cache(0);

         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  byte[] hcc = randomBytes(KEY_SIZE);
                  bchm.put(hcc, hcc);
               }
               return null;
            });
         }

         for (int i = 0; i < REMOVE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < REMOVECOUNT; ++j) {
                  Iterator<Map.Entry<byte[], byte[]>> iterator = bchm.entrySet().iterator();
                  while (iterator.hasNext()) {
                     bchm.remove(iterator.next().getKey());
                  }
               }
               return null;
            });
         }

         for (int i = 0; i < EXECUTE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < EXECUTECOUNT; ++j) {
                  DataContainer<WrappedByteArray, WrappedByteArray> container =
                        castDC(bchm.getAdvancedCache().getDataContainer());
                  container.forEach(ice -> assertEquals(ice, container.get(ice.getKey())));
               }
               return null;
            });
         }

         for (int i = 0; i < WRITE_THREADS + REMOVE_THREADS + EXECUTE_THREADS; ++i) {
            Future<Void> future = service.poll(10, TimeUnit.SECONDS);
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

   static final int KEY_SIZE = 20;

   byte[] randomBytes(int size) {
      byte[] bytes = new byte[size];
      ThreadLocalRandom.current().nextBytes(bytes);
      return bytes;
   }
}
