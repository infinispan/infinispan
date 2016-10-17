package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
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
import org.infinispan.container.DataContainer;
import org.infinispan.container.StorageType;
import org.infinispan.filter.KeyFilter;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.OffHeapMultiNodeTest")
public class OffHeapMultiNodeTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.memory().storageType(StorageType.OFF_HEAP);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   public void testPutMapCommand() {
      Map<String, String> map = new HashMap<>();
      for (int i = 0; i < numberOfKeys; ++i) {
         map.put("key" + i, "value" + i);
      }

      cache(0).putAll(map);

      for (int i = 0; i < numberOfKeys; ++i) {
         assertEquals("value" + i, cache(0).get("key" + i));
      }
   }

   public void testPutRemovePut() {
      Map<byte[], byte[]> map = cache(0);
      byte[] key = randomBytes(KEY_SIZE);
      byte[] value = randomBytes(VALUE_SIZE);
      byte[] value2 = randomBytes(VALUE_SIZE);

      assertNull(map.put(key, value));

      for (int i = 0; i < 100; ++i) {
         map.put(randomBytes(KEY_SIZE), value);
      }

      assertEquals(value, map.remove(key));

      assertNull(map.put(key, value));

      assertEquals(value, map.remove(key));

      assertNull(map.put(key, value2));
   }

   public void testOverwriteSameKey() {
      Map<byte[], byte[]> map = cache(0);
      byte[] key = randomBytes(KEY_SIZE);
      byte[] value = randomBytes(VALUE_SIZE);
      byte[] value2 = randomBytes(VALUE_SIZE);
      byte[] value3 = randomBytes(VALUE_SIZE);
      assertNull(map.put(key, value));
      byte[] prev = map.put(key, value2);
      assertTrue(Arrays.equals(prev, value));
      assertTrue(Arrays.equals(value2, map.put(key, value3)));
      assertTrue(Arrays.equals(value3, map.get(key)));
   }

   public void testClear() {
      Map<String, String> map = cache(0);
      int size = 10;
      for (int i = 0; i < 10; ++i) {
         map.put("key-" + i, "value-" + i);
      }
      assertEquals(size, map.size());
      for (int i = 0; i < 10; ++i) {
         assertEquals("value-" + i, map.get("key-" + i));
      }
      map.clear();
      assertEquals(0, map.size());
      for (int i = 0; i < 10; ++i) {
         map.put("key-" + i, "value-" + i);
      }
      assertEquals(size, map.size());
   }

   public void testIterate() {
      int cacheSize = 50;
      Map<byte[], byte[]> original = new HashMap<>();
      for (int i = 0; i < cacheSize; ++i) {
         byte[] key = randomBytes(KEY_SIZE);
         original.put(key, randomBytes(VALUE_SIZE));
      }

      Map<byte[], byte[]> map = cache(0);

      map.putAll(original);
      Iterator<Map.Entry<byte[], byte[]>> iterator = map.entrySet().iterator();
      iterator.forEachRemaining(e -> assertEquals(e.getValue(), map.get(e.getKey())));
   }

   static WrappedByteArray castToWBA(Object obj) {
      return (WrappedByteArray) obj;
   }

   static DataContainer<WrappedByteArray, WrappedByteArray> castDC(Object obj) {
      return (DataContainer<WrappedByteArray, WrappedByteArray>) obj;
   }

   public void testLotsOfWritesAndFewRemoves() throws Exception {
      final int WRITE_THREADS = 5;
      final int REMOVE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int REMOVECOUNT = 1;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + REMOVE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

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

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + REMOVE_THREADS + EXECUTE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

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
                  container.executeTask(KeyFilter.ACCEPT_ALL_FILTER, (k, e) -> {
                     assertEquals(k, e.getKey());
                  });
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

   final static int KEY_SIZE = 20;
   final static int VALUE_SIZE = 1024;

   byte[] randomBytes(int size) {
      byte[] bytes = new byte[size];
      ThreadLocalRandom.current().nextBytes(bytes);
      return bytes;
   }
}
