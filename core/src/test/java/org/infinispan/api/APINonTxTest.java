package org.infinispan.api;

import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.assertNoLocks;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commons.util.ObjectDuplicator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "api.APINonTxTest")
public class APINonTxTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testConvenienceMethods() {
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<String, String>();
      data.put(key, value);

      assertNull(cache.get(key));
      assertCacheIsEmpty();

      cache.put(key, value);

      assertEquals(value, cache.get(key));
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));
      assertCacheSize(1);

      cache.remove(key);

      assertNull(cache.get(key));
      assertCacheIsEmpty();

      cache.putAll(data);

      assertEquals(value, cache.get(key));
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));
      assertCacheSize(1);
   }

   public void testStopClearsData() throws Exception {
      String key = "key", value = "value";
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      assertCacheSize(1);
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));

      cache.stop();
      assertEquals(ComponentStatus.TERMINATED, cache.getStatus());
      cache.start();

      assertFalse(cache.containsKey(key));
      assertFalse(cache.keySet().contains(key));
      assertFalse(cache.values().contains(value));
      assertCacheIsEmpty();
   }

   /**
    * Tests basic eviction
    */
   public void testEvict() {
      String key1 = "keyOne", key2 = "keyTwo", value = "value";

      cache.put(key1, value);
      cache.put(key2, value);

      assertTrue(cache.containsKey(key1));
      assertTrue(cache.containsKey(key2));
      assertCacheSize(2);

      assertTrue(cache.keySet().contains(key1));
      assertTrue(cache.keySet().contains(key2));
      assertTrue(cache.values().contains(value));

      // evict two
      cache.evict(key2);

      assertTrue(cache.containsKey(key1));
      assertFalse(cache.containsKey(key2));
      assertCacheSize(1);

      assertTrue(cache.keySet().contains(key1));
      assertFalse(cache.keySet().contains(key2));
      assertTrue(cache.values().contains(value));

      cache.evict(key1);

      assertFalse(cache.containsKey(key1));
      assertFalse(cache.containsKey(key2));
      assertFalse(cache.keySet().contains(key1));
      assertFalse(cache.keySet().contains(key2));
      assertFalse(cache.values().contains(value));
      assertCacheIsEmpty();
   }


   public void testUnsupportedKeyValueCollectionOperationsAddMethod() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Object> keys = cache.keySet();
      Collection<Object> values = cache.values();
      Collection[] collections = new Collection[]{keys, values};

      Object newObj = new Object();
      List newObjCol = new ArrayList();
      newObjCol.add(newObj);
      for (Collection col : collections) {
         expectException(UnsupportedOperationException.class, () -> col.add(newObj));
         expectException(UnsupportedOperationException.class, () -> col.addAll(newObjCol));
      }
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testAddMethodsForEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();

      Map.Entry entry = new ImmortalCacheEntry("4", "four");
      entries.add(entry);
   }

   public void testRemoveMethodOfKeyValueEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Object> keys = cache.keySet();
      keys.remove(key1);

      assertCacheSize(2);

      Collection<Object> values = cache.values();
      values.remove(value2);

      assertCacheSize(1);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.remove(new ImmortalCacheEntry(key3, value3));

      assertCacheIsEmpty();
   }

   public void testClearMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Object> keys = cache.keySet();
      keys.clear();

      assertCacheIsEmpty();
   }

   public void testClearMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Collection<Object> values = cache.values();
      values.clear();

      assertCacheIsEmpty();
   }

   public void testClearMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.clear();

      assertCacheIsEmpty();
   }

   public void testRemoveAllMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<String> keyCollection = new ArrayList<>(2);

      keyCollection.add(key2);
      keyCollection.add(key3);

      Collection<Object> keys = cache.keySet();
      keys.removeAll(keyCollection);

      assertCacheSize(1);
   }

   public void testRemoveAllMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<String> valueCollection = new ArrayList<>(2);

      valueCollection.add(value1);
      valueCollection.add(value2);

      Collection<Object> values = cache.values();
      values.removeAll(valueCollection);

      assertCacheSize(1);
   }

   public void testRemoveAllMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<Map.Entry> entryCollection = new ArrayList<>(2);

      entryCollection.add(new ImmortalCacheEntry(key1, value1));
      entryCollection.add(new ImmortalCacheEntry(key3, value3));

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.removeAll(entryCollection);

      assertCacheSize(1);
   }

   public void testRetainAllMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<String> keyCollection = new ArrayList<>(2);

      keyCollection.add(key2);
      keyCollection.add(key3);
      keyCollection.add("6");

      Collection<Object> keys = cache.keySet();
      keys.retainAll(keyCollection);

      assertCacheSize(2);
   }

   public void testRetainAllMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<String> valueCollection = new ArrayList<>(2);

      valueCollection.add(value1);
      valueCollection.add(value2);
      valueCollection.add("5");

      Collection<Object> values = cache.values();
      values.retainAll(valueCollection);

      assertCacheSize(2);
   }

   public void testRetainAllMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<Map.Entry> entryCollection = new ArrayList<>(3);

      entryCollection.add(new ImmortalCacheEntry(key1, value1));
      entryCollection.add(new ImmortalCacheEntry(key3, value3));
      entryCollection.add(new ImmortalCacheEntry("4", "5"));

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.retainAll(entryCollection);

      assertCacheSize(2);
   }

   public void testEntrySetValueFromEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      Object newObj = new Object();

      for (Map.Entry entry : entries) {
         entry.setValue(newObj);
      }

      assertCacheSize(3);

      assertEquals(newObj, cache.get(key1));
      assertEquals(newObj, cache.get(key2));
      assertEquals(newObj, cache.get(key3));
   }

   public void testKeyValueEntryCollections() {
      String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      assertCacheSize(3);

      Set expKeys = new HashSet();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);

      Set expValues = new HashSet();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);

      Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set expValueEntries = ObjectDuplicator.duplicateSet(expValues);

      Set<Object> keys = cache.keySet();
      for (Object key : keys) {
         assertTrue(expKeys.remove(key));
      }
      assertTrue(expKeys.isEmpty(), "Did not see keys " + expKeys + " in iterator!");

      Collection<Object> values = cache.values();
      for (Object value : values) {
         assertTrue(expValues.remove(value));
      }
      assertTrue(expValues.isEmpty(), "Did not see keys " + expValues + " in iterator!");

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      for (Map.Entry entry : entries) {
         assertTrue(expKeyEntries.remove(entry.getKey()));
         assertTrue(expValueEntries.remove(entry.getValue()));
      }
      assertTrue(expKeyEntries.isEmpty(), "Did not see keys " + expKeyEntries + " in iterator!");
      assertTrue(expValueEntries.isEmpty(), "Did not see keys " + expValueEntries + " in iterator!");
   }

   public void testSizeAndContents() throws Exception {
      String key = "key", value = "value";

      assertCacheIsEmpty();
      assertFalse(cache.containsKey(key));
      assertFalse(cache.keySet().contains(key));
      assertFalse(cache.values().contains(value));

      cache.put(key, value);
      assertCacheSize(1);

      assertTrue(cache.containsKey(key));
      assertTrue(cache.containsKey(key));
      assertTrue(cache.keySet().contains(key));
      assertTrue(cache.values().contains(value));

      assertEquals(value, cache.remove(key));

      assertTrue(cache.isEmpty());
      assertCacheIsEmpty();

      assertFalse(cache.containsKey(key));
      assertFalse(cache.keySet().contains(key));
      assertFalse(cache.values().contains(value));

      Map<String, String> m = new HashMap<>();
      m.put("1", "one");
      m.put("2", "two");
      m.put("3", "three");
      cache.putAll(m);

      assertEquals("one", cache.get("1"));
      assertEquals("two", cache.get("2"));
      assertEquals("three", cache.get("3"));
      assertCacheSize(3);

      m = new HashMap<>();
      m.put("1", "newvalue");
      m.put("4", "four");

      cache.putAll(m);

      assertEquals("newvalue", cache.get("1"));
      assertEquals("two", cache.get("2"));
      assertEquals("three", cache.get("3"));
      assertEquals("four", cache.get("4"));

      assertCacheSize(4);
   }

   public void testConcurrentMapMethods() {

      assertNull(cache.putIfAbsent("A", "B"));
      assertEquals("B", cache.putIfAbsent("A", "C"));
      assertEquals("B", cache.get("A"));

      assertFalse(cache.remove("A", "C"));
      assertTrue(cache.containsKey("A"));
      assertTrue(cache.remove("A", "B"));
      assertFalse(cache.containsKey("A"));

      cache.put("A", "B");

      assertFalse(cache.replace("A", "D", "C"));
      assertEquals("B", cache.get("A"));
      assertTrue(cache.replace("A", "B", "C"));
      assertEquals("C", cache.get("A"));

      assertEquals("C", cache.replace("A", "X"));
      assertNull(cache.replace("X", "A"));
      assertFalse(cache.containsKey("X"));
   }

   public void testPutNullKeyParameter() {
      expectException(NullPointerException.class, () -> cache.put(null, null));
   }

   public void testPutNullValueParameter() {
      expectException(NullPointerException.class, () -> cache.put("hello", null));
   }

   public void testReplaceNullKeyParameter() {
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.replace(null, "X"));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.replace(null, "X", "Y"));
   }

   public void testReplaceNullValueParameter() {
      expectException(NullPointerException.class, "Null values are not supported!", () -> cache.replace("hello", null, "X"));
      expectException(NullPointerException.class, "Null values are not supported!", () -> cache.replace("hello", "X", null));
   }

   public void testPutIfAbsentLockCleanup() {
      assertNoLocks(cache);
      cache.put("key", "value");
      assertNoLocks(cache);
      // This call should fail.
      cache.putForExternalRead("key", "value2");
      assertNoLocks(cache);
      assertEquals("value", cache.get("key"));
   }

   private void assertCacheIsEmpty() {
      assertCacheSize(0);
   }

   private void assertCacheSize(int expectedSize) {
      assertEquals(expectedSize, cache.size());
      assertEquals(expectedSize, cache.keySet().size());
      assertEquals(expectedSize, cache.values().size());
      assertEquals(expectedSize, cache.entrySet().size());

      boolean isEmpty = expectedSize == 0;
      assertEquals(isEmpty, cache.isEmpty());
      assertEquals(isEmpty, cache.keySet().isEmpty());
      assertEquals(isEmpty, cache.values().isEmpty());
      assertEquals(isEmpty, cache.entrySet().isEmpty());
   }

   public void testGetOrDefault() {
      cache.put("A", "B");

      assertEquals("K", cache.getOrDefault("Not there", "K"));
   }

   public void testMerge() throws Exception {
      cache.put("A", "B");

      // replace
      cache.merge("A", "C", (oldValue, newValue) -> "" + oldValue + newValue);
      assertEquals("BC", cache.get("A"));

      // remove if null value after remapping
      cache.merge("A", "C", (oldValue, newValue) -> null);
      assertEquals(null, cache.get("A"));

      // put if absent
      cache.merge("F", "42", (oldValue, newValue) -> "" + oldValue + newValue);
      assertEquals("42", cache.get("F"));
   }

   public void testForEach() {
      cache.put("A", "B");
      cache.put("C", "D");

      List<String> values = new ArrayList<>();
      BiConsumer<? super Object, ? super Object> collectKeyValues = (k, v) -> values.add("hello_" + k.toString() + v.toString());

      cache.forEach(collectKeyValues);

      assertEquals(2, values.size());
      assertEquals("hello_AB", values.get(0));
      assertEquals("hello_CD", values.get(1));
   }

   public void testComputeIfAbsent() {
      Function mappingFunction = k -> k + " world";
      assertEquals("hello world", cache.computeIfAbsent("hello", mappingFunction));
      assertEquals("hello world", cache.get("hello"));

      Function functionAfterPut = k -> k + " happy";
      // hello already exists so nothing should happen
      assertEquals("hello world", cache.computeIfAbsent("hello", functionAfterPut));
      assertEquals("hello world", cache.get("hello"));

      Function functionMapsToNull = k -> null;
      assertNull( cache.computeIfAbsent("kaixo", functionMapsToNull), "with function mapping to null returns null");
      assertNull(cache.get("kaixo"), "the key does not exist");
   }

   public void testComputeIfPresent() {
      BiFunction mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.computeIfPresent("es", mappingFunction));
      assertEquals("hello_es:hola", cache.get("es"));

      BiFunction mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertNull(cache.computeIfPresent("fr", mappingForNotPresentKey), "unexisting key should return null");
      assertNull(cache.get("fr"), "unexisting key should return null");

      BiFunction mappingToNull = (k, v) -> null;
      assertNull(cache.computeIfPresent("es", mappingToNull), "mapping to null returns null");
      assertNull(cache.get("es"), "the key is removed");
   }

   public void testCompute() {
      BiFunction mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.compute("es", mappingFunction));
      assertEquals("hello_es:hola", cache.get("es"));

      BiFunction mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertEquals("absent_fr:null", cache.compute("fr", mappingForNotPresentKey));
      assertEquals("absent_fr:null", cache.get("fr"));

      BiFunction mappingToNull = (k, v) -> null;
      assertNull(cache.compute("es", mappingToNull), "mapping to null returns null");
      assertNull(cache.get("es"), "the key is removed");
   }

   public void testReplaceAll() {
      BiFunction mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");
      cache.put("cz", "ahoj");

      cache.replaceAll(mappingFunction);

      assertEquals("hello_es:hola", cache.get("es"));
      assertEquals("hello_cz:ahoj", cache.get("cz"));

      BiFunction mappingToNull = (k, v) -> null;
      expectException(NullPointerException.class, () -> cache.replaceAll(mappingToNull));

      assertEquals("hello_es:hola", cache.get("es"));
      assertEquals("hello_cz:ahoj", cache.get("cz"));
   }

   public void testFalseEqualsKey() {
      assertNull(cache.get(new FalseEqualsKey("boo", 1)));
      cache.put(new FalseEqualsKey("boo", 1), "blah");
      assertNull(cache.get(new FalseEqualsKey("boo", 1)));
   }

   static class FalseEqualsKey {
      final String name;
      final int value;

      FalseEqualsKey(String name, int value) {
         this.name = name;
         this.value = value;
      }

      @Override
      public int hashCode() {
         return 0;
      }

      @Override
      public boolean equals(Object obj) {
         return false;
      }
   }

}
