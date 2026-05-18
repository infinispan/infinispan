package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.RemoveAllCommandTest")
public class RemoveAllCommandTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).l1().disable();
      dcc.locking().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCluster(TestDataSCI.INSTANCE, dcc, 4);
      waitForClusterToForm();
   }

   public void testRemoveAll() {
      Map<String, String> map = new HashMap<>();
      for (int i = 0; i < NUM_KEYS; i++) {
         map.put("key" + i, "value" + i);
      }
      cache(0).putAll(map);

      for (int i = 0; i < NUM_KEYS; i++) {
         assertEquals("value" + i, cache(0).get("key" + i));
      }

      Set<String> keys = new HashSet<>(map.keySet());
      cache(0).removeAll(keys);

      for (int i = 0; i < NUM_KEYS; i++) {
         assertNull(cache(0).get("key" + i));
         final int fi = i;
         eventuallyEquals(null, () -> cache(1).get("key" + fi));
         eventuallyEquals(null, () -> cache(2).get("key" + fi));
         eventuallyEquals(null, () -> cache(3).get("key" + fi));
      }
   }

   public void testRemoveAllNonExistent() {
      Set<String> keys = Set.of("nonexistent1", "nonexistent2");
      cache(0).removeAll(keys);
      // No exception expected
   }

   public void testRemoveAllPartial() {
      Map<String, String> map = new HashMap<>();
      for (int i = 0; i < NUM_KEYS; i++) {
         map.put("key" + i, "value" + i);
      }
      cache(0).putAll(map);

      Set<String> keysToRemove = new HashSet<>();
      for (int i = 0; i < 5; i++) {
         keysToRemove.add("key" + i);
      }
      cache(0).removeAll(keysToRemove);

      for (int i = 0; i < 5; i++) {
         assertNull(cache(0).get("key" + i));
      }
      for (int i = 5; i < NUM_KEYS; i++) {
         assertEquals("value" + i, cache(0).get("key" + i));
      }
   }

   public void testRemoveAllEmpty() {
      cache(0).put("key", "value");
      cache(0).removeAll(Collections.emptySet());
      assertEquals("value", cache(0).get("key"));
   }
}
