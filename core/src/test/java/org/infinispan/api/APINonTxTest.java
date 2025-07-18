package org.infinispan.api;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.test.TestingUtil.assertNoLocks;
import static org.infinispan.test.TestingUtil.createMapEntry;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.LockedStream;
import org.infinispan.commons.lambda.NamedLambdas;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "api.APINonTxTest")
public class APINonTxTest extends SingleCacheManagerTest {

   protected void configure(ConfigurationBuilder builder) {
   }

   @AfterMethod
   public void checkForLeakedTransactions() {
      TransactionTable txTable = TestingUtil.getTransactionTable(cache);
      if (txTable != null) {
         assertEquals(0, txTable.getLocalTxCount());
         assertEquals(0, txTable.getLocalTransactions().size());
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      configure(c);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(true, TestDataSCI.INSTANCE);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testConvenienceMethods() {
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<>();
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

   public void testStopClearsData() {
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

      // We should be fine if we evict a non existent key
      cache.evict(key1);
   }


   public void testUnsupportedKeyValueCollectionOperationsAddMethod() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Object> keys = cache.keySet();
      Collection<Object> values = cache.values();
      //noinspection unchecked
      Collection<Object>[] collections = new Collection[]{keys, values};

      String newObj = "4";
      List<Object> newObjCol = new ArrayList<>();
      newObjCol.add(newObj);
      for (Collection<Object> col : collections) {
         expectException(UnsupportedOperationException.class, () -> col.add(newObj));
         expectException(UnsupportedOperationException.class, () -> col.addAll(newObjCol));
      }
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testAddMethodsForEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();

      entries.add(createMapEntry("4", "four"));
   }

   public void testRemoveMethodOfKeyValueEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
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
      entries.remove(TestingUtil.<Object, Object>createMapEntry(key3, value3));

      assertCacheIsEmpty();
   }

   public void testClearMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
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
      Map<String, String> m = new HashMap<>();
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
      Map<String, String> m = new HashMap<>();
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
      Map<String, String> m = new HashMap<>();
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
      Map<String, String> m = new HashMap<>();
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
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      List<Map.Entry> entryCollection = new ArrayList<>(2);

      entryCollection.add(createMapEntry(key1, value1));
      entryCollection.add(createMapEntry(key3, value3));

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.removeAll(entryCollection);

      assertCacheSize(1);
   }

   public void testRetainAllMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
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

      entryCollection.add(createMapEntry(key1, value1));
      entryCollection.add(createMapEntry(key3, value3));
      entryCollection.add(createMapEntry("4", "5"));

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.retainAll(entryCollection);

      assertCacheSize(2);
   }

   public void testRemoveIfMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Collection<Object> keys = cache.keySet();
      keys.removeIf(k -> k.equals("2"));

      assertCacheSize(2);
   }

   public void testRemoveIfMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Collection<Object> values = cache.values();
      values.removeIf(v -> ((String) v).startsWith("t"));

      assertCacheSize(1);
   }

   public void testRemoveIfMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      entries.removeIf(e -> ((String) e.getValue()).startsWith("t"));

      assertCacheSize(1);
   }

   public void testEntrySetValueFromEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache.putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache.entrySet();
      String newObj = "something-else";

      for (Map.Entry<Object, Object> entry : entries) {
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

      Set<Object> expKeys = new HashSet<>();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);

      Set<Object> expValues = new HashSet<>();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);

      Set expKeyEntries = new HashSet(expKeys);
      Set expValueEntries = new HashSet(expValues);

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

   public void testSizeAndContents() {
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

   @SuppressWarnings("ConstantConditions")
   public void testPutNullParameters() {
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.put(null, null));
      expectException(NullPointerException.class, "Null values are not supported!", () -> cache.put("k", null));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.put(null, "v"));

      //put if absent since it shares the same command as put
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.putIfAbsent(null, null));
      expectException(NullPointerException.class, "Null values are not supported!", () -> cache.putIfAbsent("k", null));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.putIfAbsent(null, "v"));
   }

   @SuppressWarnings("ConstantConditions")
   public void testReplaceNullParameters() {
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.replace(null, null));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.replace(null, "X"));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.replace(null, "X", "Y"));
      expectException(NullPointerException.class, "Null values are not supported!",
            () -> cache.replace("hello", null, "X"));
      expectException(NullPointerException.class, "Null values are not supported!",
            () -> cache.replace("hello", "X", null));
   }

   @SuppressWarnings("ConstantConditions")
   public void testRemoveNullParameters() {
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.remove(null));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.remove(null, "X"));
      expectException(NullPointerException.class, "Null values are not supported!", () -> cache.remove("k", null));
      expectException(NullPointerException.class, "Null keys are not supported!", () -> cache.remove(null, null));
   }

   @SuppressWarnings("ConstantConditions")
   public void testComputeNullParameters() {
      expectException(NullPointerException.class, "Null keys are not supported!",
            () -> cache.compute(null, (o, o2) -> "X"));
      expectException(NullPointerException.class, "Null functions are not supported!", () -> cache.compute("k", null));

      expectException(NullPointerException.class, "Null keys are not supported!",
            () -> cache.computeIfAbsent(null, o -> "X"));
      expectException(NullPointerException.class, "Null functions are not supported!",
            () -> cache.computeIfAbsent("k", null));

      expectException(NullPointerException.class, "Null keys are not supported!",
            () -> cache.computeIfPresent(null, (o, o2) -> "X"));
      expectException(NullPointerException.class, "Null functions are not supported!",
            () -> cache.computeIfPresent("k", null));
   }

   @SuppressWarnings("ConstantConditions")
   public void testMergeNullParameters() {
      expectException(NullPointerException.class, "Null keys are not supported!",
            () -> cache.merge(null, "X", (o, o2) -> "Y"));
      expectException(NullPointerException.class, "Null values are not supported!",
            () -> cache.merge("k", null, (o, o2) -> "Y"));
      expectException(NullPointerException.class, "Null functions are not supported!",
            () -> cache.merge("k", "X", null));
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
      assertNull(cache.get("A"));

      // put if absent
      cache.merge("F", "42", (oldValue, newValue) -> "" + oldValue + newValue);
      assertEquals("42", cache.get("F"));

      cache.put("A", "B");
      RuntimeException mergeRaisedException = new RuntimeException("hi there");
      expectException(RuntimeException.class, "hi there", () -> cache.merge("A", "C", (k, v) -> {
         throw mergeRaisedException;
      }));
   }

   public void testMergeWithExpirationParameters() {
      BiFunction<Object, Object, String> mappingFunction = (v1, v2) -> v1 + " " + v2;
      cache.put("es", "hola");

      assertEquals("hola guy", cache.merge("es", "guy", mappingFunction, 1_000_000, TimeUnit.SECONDS));
      CacheEntry<Object,  Object> entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hola guy", entry.getValue());
      assertEquals(1_000_000_000, entry.getLifespan());


      assertEquals("hola guy and good bye", cache.merge("es", "and good bye", mappingFunction, 1_100_000, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
      entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hola guy and good bye", entry.getValue());
      assertEquals(1_100_000_000, entry.getLifespan());
   }

   public void testForEach() {
      cache.put("A", "B");
      cache.put("C", "D");

      Set<String> values = new HashSet<>();
      BiConsumer<? super Object, ? super Object> collectKeyValues = (k, v) -> values.add("hello_" + k.toString() + v.toString());

      cache.forEach(collectKeyValues);

      assertEquals(TestingUtil.setOf("hello_AB", "hello_CD"), values);
   }

   public void testComputeIfAbsent() {
      Function<Object, String> mappingFunction = k -> k + " world";
      assertEquals("hello world", cache.computeIfAbsent("hello", mappingFunction));
      assertEquals("hello world", cache.get("hello"));

      Function<Object, String> functionAfterPut = k -> k + " happy";
      // hello already exists so nothing should happen
      assertEquals("hello world", cache.computeIfAbsent("hello", functionAfterPut));
      assertEquals("hello world", cache.get("hello"));

      int cacheSizeBeforeNullValueCompute = cache.size();
      Function<Object, String> functionMapsToNull = k -> null;
      assertNull("with function mapping to null returns null", cache.computeIfAbsent("kaixo", functionMapsToNull));
      assertNull("the key does not exist", cache.get("kaixo"));
      assertEquals(cacheSizeBeforeNullValueCompute, cache.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      Function<Object, String> functionMapsToException = k -> {
         throw computeRaisedException;
      };
      expectException(RuntimeException.class, "hi there", () -> cache.computeIfAbsent("es", functionMapsToException));
   }

   public void testComputeIfAbsentWithExpirationParameters() {
      Function<Object, String> mappingFunction = k -> k + " world";
      assertEquals("hello world", cache.computeIfAbsent("hello", mappingFunction, 1_000_000, TimeUnit.SECONDS));
      CacheEntry<Object,  Object> entry = cache.getAdvancedCache().getCacheEntry("hello");
      assertEquals("hello world", entry.getValue());
      assertEquals(1_000_000_000, entry.getLifespan());

      assertEquals("hello world", cache.computeIfAbsent("hello", mappingFunction, 1_100_000, TimeUnit.SECONDS,
            -1, TimeUnit.SECONDS));

      entry = cache.getAdvancedCache().getCacheEntry("hello");
      assertEquals("hello world", entry.getValue());
      // The computeIfAbsent will fail, leaving the expiration the same
      assertEquals(1_000_000_000, entry.getLifespan());
   }

   public void testComputeIfPresent() {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.computeIfPresent("es", mappingFunction));
      assertEquals("hello_es:hola", cache.get("es"));

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(RuntimeException.class, "hi there", () -> cache.computeIfPresent("es", mappingToException));

      BiFunction<Object, Object, String> mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertNull("unexisting key should return null", cache.computeIfPresent("fr", mappingForNotPresentKey));
      assertNull("unexisting key should return null", cache.get("fr"));

      BiFunction<Object, Object, String> mappingToNull = (k, v) -> null;
      assertNull("mapping to null returns null", cache.computeIfPresent("es", mappingToNull));
      assertNull("the key is removed", cache.get("es"));
   }

   public void testComputeIfPresentWithExpirationParameters() {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.computeIfPresent("es", mappingFunction, 1_000_000, TimeUnit.SECONDS));
      CacheEntry<Object,  Object> entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hello_es:hola", entry.getValue());
      assertEquals(1_000_000_000, entry.getLifespan());

      assertEquals("hello_es:hello_es:hola", cache.computeIfPresent("es", mappingFunction, 1_100_000, TimeUnit.SECONDS, -1, TimeUnit.SECONDS));
      entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hello_es:hello_es:hola", entry.getValue());
      assertEquals(1_100_000_000, entry.getLifespan());
   }

   public void testCompute() {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.compute("es", mappingFunction));
      assertEquals("hello_es:hola", cache.get("es"));

      BiFunction<Object, Object, String> mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertEquals("absent_fr:null", cache.compute("fr", mappingForNotPresentKey));
      assertEquals("absent_fr:null", cache.get("fr"));

      BiFunction<Object, Object, String> mappingToNull = (k, v) -> null;
      assertNull("mapping to null returns null", cache.compute("es", mappingToNull));
      assertNull("the key is removed", cache.get("es"));

      int cacheSizeBeforeNullValueCompute = cache.size();
      assertNull("mapping to null returns null", cache.compute("eus", mappingToNull));
      assertNull("the key does not exist", cache.get("eus"));
      assertEquals(cacheSizeBeforeNullValueCompute, cache.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(RuntimeException.class, "hi there", () -> cache.compute("es", mappingToException));
   }

   public void testComputeWithExpirationParameters() {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");

      assertEquals("hello_es:hola", cache.compute("es", mappingFunction, 1_000_000, TimeUnit.SECONDS));
      CacheEntry<Object,  Object> entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hello_es:hola", entry.getValue());
      assertEquals(1_000_000_000, entry.getLifespan());

      assertEquals("hello_es:hello_es:hola", cache.compute("es", mappingFunction, 1_100_000, TimeUnit.SECONDS,
            -1, TimeUnit.SECONDS));
      entry = cache.getAdvancedCache().getCacheEntry("es");
      assertEquals("hello_es:hello_es:hola", entry.getValue());
      assertEquals(1_100_000_000, entry.getLifespan());
   }

   public void testReplaceAll() {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      cache.put("es", "hola");
      cache.put("cz", "ahoj");

      cache.replaceAll(mappingFunction);

      assertEquals("hello_es:hola", cache.get("es"));
      assertEquals("hello_cz:ahoj", cache.get("cz"));

      BiFunction<Object, Object, String> mappingToNull = (k, v) -> null;
      expectException(NullPointerException.class, () -> cache.replaceAll(mappingToNull));

      assertEquals("hello_es:hola", cache.get("es"));
      assertEquals("hello_cz:ahoj", cache.get("cz"));
   }

   @DataProvider(name = "lockedStreamActuallyLocks")
   public Object[][] lockStreamActuallyLocks() {

      List<BiConsumer<Cache<Object, Object>, CacheEntry<Object, Object>>> biConsumers = Arrays.asList(
            // Put
            NamedLambdas.of("put", (c, e) -> assertEquals("value" + e.getKey(), c.put(e.getKey(), e.getValue() + "-other"))),
            // Functional Command
            NamedLambdas.of("functional-command", (c, e) -> {
               FunctionalMap.ReadWriteMap<Object, Object> rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(c.getAdvancedCache()));
               try {
                  assertEquals("value" + e.getKey(), rwMap.eval(e.getKey(), view -> {
                     Object prev = view.get();
                     view.set(prev + "-other");
                     return prev;
                  }).get());
               } catch (InterruptedException | ExecutionException e1) {
                  throw new AssertionError(e1);
               }
            }),
            // Put all
            NamedLambdas.of("put-all", (c, e) -> c.putAll(Collections.singletonMap(e.getKey(), e.getValue() + "-other"))),
            // Put Async
            NamedLambdas.of("put-async", (c, e) -> {
               try {
                  c.putAsync(e.getKey(), e.getValue() + "-other").get(10, TimeUnit.SECONDS);
               } catch (InterruptedException | ExecutionException | TimeoutException e1) {
                  throw new AssertionError(e1);
               }
            }),
            // Compute
            NamedLambdas.of("compute", (c, e) -> c.compute(e.getKey(), (k, v) -> v + "-other")),
            // Compute if present
            NamedLambdas.of("compute-if-present", (c, e) -> c.computeIfPresent(e.getKey(), (k, v) -> v + "-other")),
            // Merge
            NamedLambdas.of("merge", (c, e) -> c.merge(e.getKey(), "-other", (v1, v2) -> "" + v1 + v2))
      );
      return biConsumers.stream().flatMap(consumer ->
            Stream.of(Boolean.TRUE, Boolean.FALSE).map(bool -> new Object[] { consumer, bool })
      ).toArray(Object[][]::new);
   }

   @Test(dataProvider = "lockedStreamActuallyLocks")
   public void testLockedStreamActuallyLocks(BiConsumer<Cache<Object, Object>, CacheEntry<Object, Object>> consumer,
         boolean forEachOrInvokeAll) throws Throwable {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      CyclicBarrier barrier = new CyclicBarrier(2);

      int key = 4;

      LockedStream<Object, Object> stream = cache.getAdvancedCache().lockedStream();
      SerializableBiConsumer<Cache<Object, Object>, CacheEntry<Object, Object>> serConsumer = (c, e) -> {
         Object innerKey = e.getKey();
         if (innerKey.equals(key)) {
            try {
               barrier.await(10, TimeUnit.SECONDS);
               consumer.accept(c, e);

               InfinispanLock lock = TestingUtil.extractComponent(c, LockManager.class).getLock(innerKey);
               assertNotNull(lock);
               assertEquals(innerKey, lock.getLockOwner());

               barrier.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException | BrokenBarrierException | TimeoutException e1) {
               throw new RuntimeException(e1);
            }
         }
      };
      Future<?> forEachFuture = fork(() -> {
         if (forEachOrInvokeAll) {
            stream.forEach(serConsumer);
         } else {
            stream.invokeAll((c, e) -> {
               serConsumer.accept(c, e);
               return null;
            });
         }
      });

      barrier.await(10, TimeUnit.SECONDS);


      Future<Object> putFuture = fork(() -> cache.put(key, "value" + key + "-new"));

      TestingUtil.assertNotDone(putFuture);

      // Let the forEach with lock complete
      barrier.await(10, TimeUnit.SECONDS);

      forEachFuture.get(10, TimeUnit.SECONDS);

      // The put should replace the value that forEach inserted
      assertEquals("value" + key + "-other", putFuture.get(10, TimeUnit.SECONDS));
      // The put should be last since it had to wait until lock was released on forEachWithLock
      assertEquals("value" + key + "-new", cache.get(key));

      // Make sure the locks were cleaned up properly
      LockManager lockManager = TestingUtil.extractComponent(cache, LockManager.class);
      assertEquals(0, lockManager.getNumberOfLocksHeld());
   }

   public void testLockedStreamSetValue() {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      cache.getAdvancedCache().lockedStream().forEach((c, e) -> e.setValue(e.getValue() + "-changed"));

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + "-changed", cache.get(i));
      }
   }

   public void testLockedStreamWithinLockedStream() {
      cache.getAdvancedCache().lockedStream()
           .forEach((c, e) -> expectException(IllegalArgumentException.class,
                                              () -> c.getAdvancedCache().lockedStream()));
   }

   private <R> void assertLockStreamInvokeAll(LockedStream<Object, Object> lockedStream,
         SerializableBiFunction<Cache<Object, Object>, CacheEntry<Object, Object>, R> biFunction,
         Map<Object, R> expectedResults) {
      Map<Object, R> results = lockedStream.invokeAll(biFunction);
      assertEquals(expectedResults, results);
   }

   public void testLockedStreamInvokeAllPut() {
      Map<Object, Object> original = new HashMap<>();
      int insertedAmount = 5;
      for (int i = 0; i < insertedAmount; i++) {
         original.put("key-" + i, "value-" + i);
      }
      cache.putAll(original);

      assertLockStreamInvokeAll(cache.getAdvancedCache().lockedStream(),
            (c, e) -> c.put(e.getKey(), e.getValue() + "-updated"), original);
      // Verify contents were updated
      for(int i = 0; i < insertedAmount; i++) {
         assertEquals("value-" + i + "-updated", cache.get("key-" + i));
      }
   }

   public void testLockedStreamInvokeAllFilteredSet() {
      Map<Object, Object> original = new HashMap<>();
      int insertedAmount = 5;
      for (int i = 0; i < insertedAmount; i++) {
         original.put("key-" + i, "value-" + i);
      }
      cache.putAll(original);

      // We only update the key with numbers 3
      assertLockStreamInvokeAll(cache.getAdvancedCache().lockedStream().filter(e -> e.getKey().toString().contains("3")),
            (c, e) -> c.put(e.getKey(), e.getValue() + "-updated"), Collections.singletonMap("key-" + 3, "value-" + 3));
      // Verify contents were updated
      for(int i = 0; i < insertedAmount; i++) {
         assertEquals("value-" + i + (i == 3 ? "-updated" : ""), cache.get("key-" + i));
      }
   }

   public void testPutMetadata() {
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      cache.getAdvancedCache().put(k(), v(), metadata);
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(v(), entry.getValue());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testPutAllMetadata() {
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      cache.getAdvancedCache().putAll(Map.of(k(), v(), k(1), v(1)), metadata);
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(v(), entry.getValue());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(v(1), entry.getValue());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testComputeAsync() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeAsync(k(), (key, value) -> value + "x")));
   }

   public void testComputeAsyncMetadata() {
      cache.put(k(), v());
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertEquals(v() + "x", await(cache.getAdvancedCache().computeAsync(k(), (key, value) -> value + "x", metadata)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testComputeAsyncLifespan() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeAsync(k(), (key, value) -> value + "x", 1, TimeUnit.DAYS)));
      assertEquals(TimeUnit.DAYS.toMillis(1), cache.getAdvancedCache().getCacheEntry(k()).getLifespan());
   }

   public void testComputeAsyncLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeAsync(k(), (key, value) -> value + "x", 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      assertEquals(TimeUnit.DAYS.toMillis(1), cache.getAdvancedCache().getCacheEntry(k()).getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), cache.getAdvancedCache().getCacheEntry(k()).getMaxIdle());
   }

   public void testComputeIfAbsentAsync() {
      cache.put(k(), v());
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertEquals(v(), await(cache.getAdvancedCache().computeIfAbsentAsync(k(), key -> "not a value", metadata)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(-1, entry.getLifespan());
      assertEquals(-1, entry.getMaxIdle());
      String v1 = v(1);
      assertEquals(v1, await(cache.getAdvancedCache().computeIfAbsentAsync(k(1), key -> v1, metadata)));
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testComputeIfAbsentAsyncMetadata() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.computeIfAbsentAsync(k(), key -> "not a value")));
      String v1 = v(1);
      assertEquals(v1, await(cache.computeIfAbsentAsync(k(1), key -> v1)));
   }

   public void testComputeIfAbsentAsyncLifespan() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.computeIfAbsentAsync(k(), key -> "not a value", 1, TimeUnit.DAYS)));
      assertEquals(-1, cache.getAdvancedCache().getCacheEntry(k()).getLifespan());
      String v1 = v(1);
      assertEquals(v1, await(cache.computeIfAbsentAsync(k(1), key -> v1, 1, TimeUnit.DAYS)));
      assertEquals(TimeUnit.DAYS.toMillis(1), cache.getAdvancedCache().getCacheEntry(k(1)).getLifespan());
   }

   public void testComputeIfAbsentAsyncLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.computeIfAbsentAsync(k(), key -> "not a value", 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      assertEquals(-1, cache.getAdvancedCache().getCacheEntry(k()).getLifespan());
      assertEquals(-1, cache.getAdvancedCache().getCacheEntry(k()).getMaxIdle());
      String v1 = v(1);
      assertEquals(v1, await(cache.computeIfAbsentAsync(k(1), key -> v1, 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testComputeIfPresentAsync() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeIfPresentAsync(k(), (key, value) -> value + "x")));
      assertNull(await(cache.computeIfPresentAsync(k(1), (key, value) -> value + "x")));
   }

   public void testComputeIfPresentAsyncMetadata() {
      cache.put(k(), v());
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertEquals(v() + "x", await(cache.getAdvancedCache().computeIfPresentAsync(k(), (key, value) -> value + "x", metadata)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(await(cache.getAdvancedCache().computeIfPresentAsync(k(1), (key, value) -> value + "x", metadata)));
   }

   public void testComputeIfPresentAsyncLifespan() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeIfPresentAsync(k(), (key, value) -> value + "x", 1, TimeUnit.DAYS)));
      assertEquals(TimeUnit.DAYS.toMillis(1), cache.getAdvancedCache().getCacheEntry(k()).getLifespan());
      assertNull(await(cache.computeIfPresentAsync(k(1), (key, value) -> value + "x")));
   }

   public void testComputeIfPresentAsyncLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.computeIfPresentAsync(k(), (key, value) -> value + "x", 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(await(cache.computeIfPresentAsync(k(1), (key, value) -> value + "x")));
   }

   public void testMergeMetadata() {
      cache.put(k(), v());
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertEquals(v() + "x", cache.getAdvancedCache().merge(k(), "x", (oldValue, value) -> (String) oldValue + value, metadata));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertEquals(v(1), cache.getAdvancedCache().merge(k(1), v(1), (oldValue, value) -> (String) oldValue + value, metadata));
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(cache.getAdvancedCache().merge(k(), "not a value", (oldValue, value) -> null, metadata));
   }

   public void testMergeAsync() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.mergeAsync(k(), "x", (oldValue, value) -> (String) oldValue + value)));
      assertEquals(v(1), await(cache.mergeAsync(k(1), v(1), (oldValue, value) -> (String) oldValue + value)));
      assertNull(await(cache.mergeAsync(k(), "not a value", (oldValue, value) -> null)));
   }

   public void testMergeAsyncMetadata() {
      cache.put(k(), v());
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertEquals(v() + "x", await(cache.getAdvancedCache().mergeAsync(k(), "x", (oldValue, value) -> (String) oldValue + value, metadata)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertEquals(v(1), await(cache.getAdvancedCache().mergeAsync(k(1), v(1), (oldValue, value) -> (String) oldValue + value, metadata)));
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(await(cache.getAdvancedCache().mergeAsync(k(), "not a value", (oldValue, value) -> null, metadata)));
   }

   public void testMergeAsyncLifespan() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.mergeAsync(k(), "x", (oldValue, value) -> (String) oldValue + value, 1, TimeUnit.DAYS)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(v(1), await(cache.mergeAsync(k(1), v(1), (oldValue, value) -> (String) oldValue + value, 1, TimeUnit.DAYS)));
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertNull(await(cache.mergeAsync(k(), "not a value", (oldValue, value) -> null)));
   }

   public void testMergeAsyncLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v() + "x", await(cache.mergeAsync(k(), "x", (oldValue, value) -> (String) oldValue + value, 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertEquals(v(1), await(cache.mergeAsync(k(1), v(1), (oldValue, value) -> (String) oldValue + value, 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(await(cache.mergeAsync(k(), "not a value", (oldValue, value) -> null)));
   }

   public void testGetAllCacheEntries() {
      cache.put(k(), v());
      cache.put(k(1), v(1));
      cache.put(k(2), v(2));
      Map<Object, CacheEntry<Object, Object>> entries = cache.getAdvancedCache().getAllCacheEntries(Set.of(k(), k(1), k(2)));
      assertEquals(3, entries.size());
      assertEquals(v(), entries.get(k()).getValue());
      assertEquals(v(1), entries.get(k(1)).getValue());
      assertEquals(v(2), entries.get(k(2)).getValue());
   }

   public void testPutAllLifespan() {
      cache.putAll(Map.of(k(), v(), k(1), v(1)), 1, TimeUnit.DAYS);
      assertEquals(v(), cache.get(k()));
      assertEquals(v(1), cache.get(k(1)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertNull(cache.getAdvancedCache().getCacheEntry(k(2)));
   }

   public void testPutAllLifespanMaxIdle() {
      cache.putAll(Map.of(k(), v(), k(1), v(1)), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS);
      assertEquals(v(), cache.get(k()));
      assertEquals(v(1), cache.get(k(1)));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      entry = cache.getAdvancedCache().getCacheEntry(k(1));
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(cache.getAdvancedCache().getCacheEntry(k(2)));
   }

   public void testReplaceLifespan() {
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
      assertFalse(cache.replace(k(), "not a value", v(1), 1, TimeUnit.DAYS));
      assertTrue(cache.replace(k(), v(), v(1), 1, TimeUnit.DAYS));
      assertEquals(v(1), cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
   }

   public void testReplaceLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
      assertFalse(cache.replace(k(), "not a value", v(1), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS));
      assertTrue(cache.replace(k(), v(), v(1), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS));
      assertEquals(v(1), cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testReplaceMetadata() {
      cache.put(k(), v());
      assertEquals(v(), cache.get(k()));
      Metadata metadata = new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.DAYS).maxIdle(1, TimeUnit.HOURS).build();
      assertFalse(cache.getAdvancedCache().replace(k(), "not a value", v(1), metadata));
      assertTrue(cache.getAdvancedCache().replace(k(), v(), v(1), metadata));
      assertEquals(v(1), cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testReplaceAsync() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.replaceAsync(k(), v() + "x")));
      assertEquals(v() + "x", cache.get(k()));
      assertNull(await(cache.replaceAsync(k(1), v(1))));
      assertNull(cache.get(k(1)));
   }

   public void testReplaceAsyncLifespan() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.replaceAsync(k(), v() + "x", 1 , TimeUnit.DAYS)));
      assertEquals(v() + "x", cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertNull(await(cache.replaceAsync(k(1), v(1), 1, TimeUnit.DAYS)));
      assertNull(cache.get(k(1)));
   }

   public void testReplaceAsyncLifespanMaxIdle() {
      cache.put(k(), v());
      assertEquals(v(), await(cache.replaceAsync(k(), v() + "x", 1 , TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      assertEquals(v() + "x", cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertNull(await(cache.replaceAsync(k(1), v(1), 1, TimeUnit.DAYS)));
      assertNull(cache.get(k(1)));
   }

   public void testReplaceAsyncOldValue() {
      cache.put(k(), v());
      assertFalse(await(cache.replaceAsync(k(), "not a value", v() + "x")));
      assertTrue(await(cache.replaceAsync(k(), v(), v() + "x")));
      assertEquals(v() + "x", cache.get(k()));
      assertFalse(await(cache.replaceAsync(k(1), v(1), v(1) + "x")));
      assertNull(cache.get(k(1)));
   }

   public void testReplaceAsyncOldValueLifespan() {
      cache.put(k(), v());
      assertFalse(await(cache.replaceAsync(k(), "not a value", v() + "x", 1 , TimeUnit.DAYS)));
      assertTrue(await(cache.replaceAsync(k(), v(), v() + "x", 1 , TimeUnit.DAYS)));
      assertEquals(v() + "x", cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertFalse(await(cache.replaceAsync(k(1), v(1), v(1) + "x", 1, TimeUnit.DAYS)));
      assertNull(cache.get(k(1)));
   }

   public void testReplaceAsyncOldValueLifespanMaxIdle() {
      cache.put(k(), v());
      assertFalse(await(cache.replaceAsync(k(), "not a value", v() + "x", 1 , TimeUnit.DAYS,1, TimeUnit.HOURS)));
      assertTrue(await(cache.replaceAsync(k(), v(), v() + "x", 1 , TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      assertEquals(v() + "x", cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      assertFalse(await(cache.replaceAsync(k(1), v(1), v(1) + "x", 1, TimeUnit.DAYS, 1, TimeUnit.HOURS)));
      assertNull(cache.get(k(1)));
   }

   public void testPutLifespanMaxIdle() {
      cache.put(k(), v(), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS);
      assertEquals(v(), cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
   }

   public void testPutIfAbsentLifespanMaxIdle() {
      cache.putIfAbsent(k(), v(), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS);
      assertEquals(v(), cache.get(k()));
      CacheEntry<Object, Object> entry = cache.getAdvancedCache().getCacheEntry(k());
      assertEquals(TimeUnit.DAYS.toMillis(1), entry.getLifespan());
      assertEquals(TimeUnit.HOURS.toMillis(1), entry.getMaxIdle());
      cache.putIfAbsent(k(), v(1), 1, TimeUnit.DAYS, 1, TimeUnit.HOURS);
      assertEquals(v(), cache.get(k()));
   }


}
