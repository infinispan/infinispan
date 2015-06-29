package org.infinispan.functional;

import org.infinispan.AdvancedCache;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.testng.AssertJUnit.*;

/**
 * Test suite for verifying that the concurrent map implementation
 * based on functional map behaves in the correct way.
 */
@Test(groups = "functional", testName = "functional.ConcurrentMapWithFunctionalMapsTest")
public class ConcurrentMapWithFunctionalMapsTest extends SingleCacheManagerTest {

   ConcurrentMap<Integer, String> map;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      AdvancedCache<Integer, String> advCache = cacheManager.<Integer, String>getCache().getAdvancedCache();
      map = new ConcurrentMapDecorator<>(FunctionalMapImpl.<Integer, String>create(advCache));
   }

   public void testEmptyGetThenPut() {
      assertEquals(null, map.get(1));
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
   }

   public void testPutGet() {
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
   }

   public void testGetAndPut() {
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.put(1, "uno"));
      assertEquals("uno", map.get(1));
   }

   public void testGetAndRemove() {
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
      assertEquals("one", map.remove(1));
      assertEquals(null, map.get(1));
   }

   public void testContainsKey() {
      assertEquals(false, map.containsKey(1));
      assertEquals(null, map.put(1, "one"));
      assertEquals(true, map.containsKey(1));
   }

   public void testContainsValue() {
      assertEquals(false, map.containsValue("one"));
      assertEquals(null, map.put(1, "one"));
      assertEquals(true, map.containsValue("one"));
      assertEquals(false, map.containsValue("uno"));
   }

   public void testSize() {
      assertEquals(0, map.size());
      assertEquals(null, map.put(1, "one"));
      assertEquals(1, map.size());
      assertEquals(null, map.put(2, "two"));
      assertEquals(null, map.put(3, "three"));
      assertEquals(3, map.size());
   }

   public void testEmpty() {
      assertEquals(true, map.isEmpty());
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
      assertEquals(false, map.isEmpty());
      assertEquals("one", map.remove(1));
      assertEquals(true, map.isEmpty());
   }

   public void testPutAll() {
      assertEquals(true, map.isEmpty());
      Map<Integer, String> data = new HashMap<>();
      data.put(1, "one");
      data.put(2, "two");
      data.put(3, "three");
      map.putAll(data);
      assertEquals("one", map.get(1));
      assertEquals("two", map.get(2));
      assertEquals("three", map.get(3));
   }

   public void testClear() {
      assertEquals(true, map.isEmpty());
      Map<Integer, String> data = new HashMap<>();
      data.put(1, "one");
      data.put(2, "two");
      data.put(3, "three");
      map.putAll(data);
      map.clear();
      assertEquals(null, map.get(1));
      assertEquals(null, map.get(2));
      assertEquals(null, map.get(3));
   }

   public void testKeyValueAndEntrySets() {
      assertEquals(true, map.isEmpty());
      Map<Integer, String> data = new HashMap<>();
      data.put(1, "one");
      data.put(2, "two");
      data.put(3, "three");
      data.put(33, "three");
      map.putAll(data);

      Set<Integer> keys = map.keySet();
      assertEquals(4, keys.size());
      Set<Integer> expectedKeys = new HashSet<>(Arrays.asList(1, 2, 3));
      keys.forEach(expectedKeys::remove);
      assertEquals(true, expectedKeys.isEmpty());

      assertEquals(false, map.isEmpty());
      Collection<String> values = map.values();
      assertEquals(4, values.size());
      Set<String> expectedValues = new HashSet<>(Arrays.asList("one", "two", "three"));
      values.forEach(expectedValues::remove);
      assertEquals(true, expectedValues.isEmpty());

      Set<Map.Entry<Integer, String>> entries = map.entrySet();
      assertEquals(4, entries.size());
      entries.removeAll(data.entrySet());
      assertEquals(true, entries.isEmpty());
   }

   public void testPutIfAbsent() {
      assertEquals(null, map.get(1));
      assertEquals(null, map.putIfAbsent(1, "one"));
      assertEquals("one", map.get(1));
      assertEquals("one", map.putIfAbsent(1, "uno"));
      assertEquals("one", map.get(1));
      assertEquals("one", map.remove(1));
      assertEquals(null, map.get(1));
   }

   public void testConditionalRemove() {
      assertEquals(null, map.get(1));
      assertFalse(map.remove(1, "xxx"));
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
      assertFalse(map.remove(1, "xxx"));
      assertEquals("one", map.get(1));
      assertTrue(map.remove(1, "one"));
      assertEquals(null, map.get(1));
   }

   public void testReplace() {
      assertEquals(null, map.get(1));
      assertEquals(null, map.replace(1, "xxx"));
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
      assertEquals("one", map.replace(1, "uno"));
      assertEquals("uno", map.get(1));
      assertEquals("uno", map.remove(1));
      assertEquals(null, map.get(1));
   }

   public void testReplaceWithValue() {
      assertEquals(null, map.get(1));
      assertFalse(map.replace(1, "xxx", "uno"));
      assertEquals(null, map.put(1, "one"));
      assertEquals("one", map.get(1));
      assertFalse(map.replace(1, "xxx", "uno"));
      assertEquals("one", map.get(1));
      assertTrue(map.replace(1, "one", "uno"));
      assertEquals("uno", map.get(1));
      assertEquals("uno", map.remove(1));
      assertEquals(null, map.get(1));
   }

}
