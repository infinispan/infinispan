package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

public class HashMapTest {
   @Test
   public void testCreateHopscotch() {
      testCreate(new HopscotchHashMap<>(32));
   }

   @Test
   public void testCreateHopscotchWithRemovals() {
      testCreateWithRemovals(new HopscotchHashMap<>(32));
   }

   private void testCreate(Map<Integer, Integer> testedMap) {
      HashMap<Integer, Integer> map = new HashMap<>();
      ThreadLocalRandom random = ThreadLocalRandom.current();
      for (int i = 0; i < 10000; ++i) {
         Integer key;
         do {
            key = random.nextInt();
         } while (map.containsKey(key));
         Integer value = random.nextInt();
         testedMap.put(key, value);
         assertEquals("Contents differs with " + (i + 1) + " entries", value, testedMap.get(key));
         assertEquals(i + 1, testedMap.size());
         map.put(key, value);
         for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            Integer actual = testedMap.get(entry.getKey());
            assertEquals(entry.getValue(), actual);
         }
      }
      assertEquals(map, testedMap);
      // test re-inserts
      for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
         testedMap.put(entry.getKey(), entry.getValue());
         assertEquals(map.size(), testedMap.size());
      }
      assertEquals(map, testedMap);
   }

   private void testCreateWithRemovals(Map<Integer, Integer> testedMap) {
      HashMap<Integer, Integer> map = new HashMap<>();
      ThreadLocalRandom random = ThreadLocalRandom.current();
      int[] keys = new int[10000];
      Arrays.fill(keys, -1);
      for (int i = 0; i < 10000; ++i) {
         if (!map.isEmpty() && random.nextInt(5) == 0) {
            int ki, key;
            do {
               key = keys[ki = random.nextInt(i)];
            } while (key < 0);
            keys[ki] = -1;
            Integer value = map.remove(key);
            assertNotNull(value);
            Integer removed = testedMap.remove(key);
            assertEquals(value, removed);
            assertEquals(map.size(), testedMap.size());
         } else {
            int key;
            do {
               key = Math.abs(random.nextInt());
            } while (map.containsKey(key));
            Integer value = random.nextInt();
            keys[i] = key;
            testedMap.put(key, value);
            assertEquals("Contents differs with " + (i + 1) + " entries", value, testedMap.get(key));
            map.put(key, value);
            assertEquals(map.size(), testedMap.size());
         }
         for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            assertEquals(entry.getValue(), testedMap.get(entry.getKey()));
         }
      }
      assertEquals(map, testedMap);
   }
}
