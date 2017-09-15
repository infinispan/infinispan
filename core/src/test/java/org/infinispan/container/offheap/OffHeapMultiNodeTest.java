package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.OffHeapMultiNodeTest")
public class OffHeapMultiNodeTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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

      for (int i = 0; i < 50; ++i) {
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

   static DataContainer<WrappedByteArray, WrappedByteArray> castDC(Object obj) {
      return (DataContainer<WrappedByteArray, WrappedByteArray>) obj;
   }

   final static int KEY_SIZE = 20;
   final static int VALUE_SIZE = 1024;

   byte[] randomBytes(int size) {
      byte[] bytes = new byte[size];
      ThreadLocalRandom.current().nextBytes(bytes);
      return bytes;
   }
}
