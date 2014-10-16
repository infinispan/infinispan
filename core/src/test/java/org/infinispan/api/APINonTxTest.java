package org.infinispan.api;

import org.infinispan.commons.util.ObjectDuplicator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.test.TestingUtil.assertNoLocks;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.APINonTxTest")
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

      assert cache.get(key) == null;
      assert cache.keySet().isEmpty();
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();

      cache.put(key, value);

      assert value.equals(cache.get(key));
      assert 1 == cache.keySet().size() && 1 == cache.values().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      cache.remove(key);

      assert cache.get(key) == null;
      assert cache.keySet().isEmpty();
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();

      cache.putAll(data);

      assert value.equals(cache.get(key));
      assert 1 == cache.keySet().size() && 1 == cache.values().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);
   }

      public void testStopClearsData() throws Exception {
      String key = "key", value = "value";
      int size = 0;
      cache.put(key, value);
      assert cache.get(key).equals(value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      cache.stop();
      assert cache.getStatus() == ComponentStatus.TERMINATED;
      cache.start();

      assert !cache.containsKey(key);
      assert cache.isEmpty();
      assert !cache.keySet().contains(key);
      assert cache.keySet().isEmpty();
      assert !cache.values().contains(value);
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();
   }

   /**
    * Tests basic eviction
    */
   public void testEvict() {
      String key1 = "keyOne", key2 = "keyTwo", value = "value";
      int size = 0;

      cache.put(key1, value);
      cache.put(key2, value);

      assert cache.containsKey(key1);
      assert cache.containsKey(key2);
      size = 2;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key1);
      assert cache.keySet().contains(key2);
      assert cache.values().contains(value);

      // evict two
      cache.evict(key2);

      assert cache.containsKey(key1);
      assert !cache.containsKey(key2);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.keySet().contains(key1);
      assert !cache.keySet().contains(key2);
      assert cache.values().contains(value);

      cache.evict(key1);

      assert !cache.containsKey(key1);
      assert !cache.containsKey(key2);
      assert cache.isEmpty();
      assert !cache.keySet().contains(key1);
      assert !cache.keySet().contains(key2);
      assert cache.keySet().isEmpty();
      assert !cache.values().contains(value);
      assert cache.values().isEmpty();
      assert cache.entrySet().isEmpty();
   }

   public void testUnsupportedKeyValueCollectionOperations() {
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
         try {
            col.add(newObj);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         } catch (ClassCastException e) {
            // Ignore class cast in expired filtered set because
            // you cannot really add an Object type instance.
         }
         try {
            col.addAll(newObjCol);
            assert false : "Should have thrown a UnsupportedOperationException";
         } catch (UnsupportedOperationException uoe) {
         }
      }
   }

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

      assertEquals(4, cache.size());

      List<Map.Entry<Object, Object>> entryCollection = new ArrayList<>(2);

      entryCollection.add(new ImmortalCacheEntry("5", "five"));
      entryCollection.add(new ImmortalCacheEntry("6", "six"));

      entries.addAll(entryCollection);

      assertEquals(6, cache.size());
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

      assertEquals(2, cache.size());

      Collection<Object> values = cache.values();
      values.remove(value2);

      assertEquals(1, cache.size());

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.remove(new ImmortalCacheEntry(key3, value3));

      assertEquals(0, cache.size());
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

      assertEquals(0, cache.size());
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

      assertEquals(0, cache.size());
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

      assertEquals(0, cache.size());
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

      assertEquals(1, cache.size());
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

      assertEquals(1, cache.size());
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

      assertEquals(1, cache.size());
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

      assertEquals(2, cache.size());
   }

   public void testRetainAllMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
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

      assertEquals(2, cache.size());
   }

   public void testRetainAllMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
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

      assertEquals(2, cache.size());
   }

   public void testEntrySetValueFromEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      Object newObj = new Object();

      for (Map.Entry entry : entries) {
         entry.setValue(newObj);
      }

      assertEquals(3, cache.size());

      assertEquals(newObj, cache.get(key1));
      assertEquals(newObj, cache.get(key2));
      assertEquals(newObj, cache.get(key3));
   }

   public void testKeyValueEntryCollections() {
      String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<String, String>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);
      assert 3 == cache.size() && 3 == cache.keySet().size() && 3 == cache.values().size() && 3 == cache.entrySet().size();

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
         assert expKeys.remove(key);
      }
      assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";

      Collection<Object> values = cache.values();
      for (Object value : values) {
         assert expValues.remove(value);
      }
      assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      for (Map.Entry entry : entries) {
         assert expKeyEntries.remove(entry.getKey());
         assert expValueEntries.remove(entry.getValue());
      }
      assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
      assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
   }

   public void testSizeAndContents() throws Exception {
      String key = "key", value = "value";
      int size = 0;

      assert cache.isEmpty();
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert !cache.containsKey(key);
      assert !cache.keySet().contains(key);
      assert !cache.values().contains(value);

      cache.put(key, value);
      size = 1;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert cache.containsKey(key);
      assert !cache.isEmpty();
      assert cache.containsKey(key);
      assert cache.keySet().contains(key);
      assert cache.values().contains(value);

      assert cache.remove(key).equals(value);

      assert cache.isEmpty();
      size = 0;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
      assert !cache.containsKey(key);
      assert !cache.keySet().contains(key);
      assert !cache.values().contains(value);

      Map<String, String> m = new HashMap<String, String>();
      m.put("1", "one");
      m.put("2", "two");
      m.put("3", "three");
      cache.putAll(m);

      assert cache.get("1").equals("one");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      size = 3;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();

      m = new HashMap<String, String>();
      m.put("1", "newvalue");
      m.put("4", "four");

      cache.putAll(m);

      assert cache.get("1").equals("newvalue");
      assert cache.get("2").equals("two");
      assert cache.get("3").equals("three");
      assert cache.get("4").equals("four");
      size = 4;
      assert size == cache.size() && size == cache.keySet().size() && size == cache.values().size() && size == cache.entrySet().size();
   }

   public void testConcurrentMapMethods() {

      assert cache.putIfAbsent("A", "B") == null;
      assert cache.putIfAbsent("A", "C").equals("B");
      assert cache.get("A").equals("B");

      assert !cache.remove("A", "C");
      assert cache.containsKey("A");
      assert cache.remove("A", "B");
      assert !cache.containsKey("A");

      cache.put("A", "B");

      assert !cache.replace("A", "D", "C");
      assert cache.get("A").equals("B");
      assert cache.replace("A", "B", "C");
      assert cache.get("A").equals("C");

      assert cache.replace("A", "X").equals("C");
      assert cache.replace("X", "A") == null;
      assert !cache.containsKey("X");
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testPutNullKeyParameter() {
      cache.put(null, null);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testPutNullValueParameter() {
      cache.put("hello", null);
   }

   public void testReplaceNullKeyParameter() {
      try {
         cache.replace(null, "X");
         fail();
      } catch (NullPointerException npe) {
         assertEquals("Null keys are not supported!", npe.getMessage());
      }

      try {
         cache.replace(null, "X", "Y");
         fail();
      } catch (NullPointerException npe) {
         assertEquals("Null keys are not supported!", npe.getMessage());
      }
   }

   public void testReplaceNullValueParameter() {
      try {
         cache.replace("hello", null, "X");
         fail();
      } catch (NullPointerException npe) {
         assertEquals("Null values are not supported!", npe.getMessage());
      }

      try {
         cache.replace("hello", "X", null);
         fail();
      } catch (NullPointerException npe) {
         assertEquals("Null values are not supported!", npe.getMessage());
      }
   }

   public void testPutIfAbsentLockCleanup() {
      assertNoLocks(cache);
      cache.put("key", "value");
      assertNoLocks(cache);
      // This call should fail.
      cache.putForExternalRead("key", "value2");
      assertNoLocks(cache);
      assert cache.get("key").equals("value");
   }
}
