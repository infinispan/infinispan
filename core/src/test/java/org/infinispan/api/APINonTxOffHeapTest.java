package org.infinispan.api;

import static org.infinispan.test.Exceptions.expectException;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.APINonTxOffHeapTest")
public class APINonTxOffHeapTest extends APINonTxTest {

   private StorageType storageType;

   public APINonTxOffHeapTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new APINonTxOffHeapTest().storageType(StorageType.BINARY),
            new APINonTxOffHeapTest().storageType(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      builder.memory().storageType(storageType);
   }

   @Test
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

      cache.put("A", "B");
      RuntimeException mergeRaisedException = new RuntimeException("hi there");
      expectException(RuntimeException.class, "hi there", () -> cache.merge("A", "C", (k, v) -> {
         throw mergeRaisedException;
      }));
   }

   @Test
   public void testForEach() {
      cache.put("A", "B");
      cache.put("C", "D");

      List<String> values = new ArrayList<>();
      BiConsumer<? super Object, ? super Object> collectKeyValues = (k, v) -> values.add("hello_" + k.toString() + v.toString());

      cache.forEach(collectKeyValues);

      assertEquals(2, values.size());
      //iteration order is not guaranteed, checking just that value is present
      assertTrue(values.contains("hello_AB"));
      assertTrue(values.contains("hello_CD"));
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
      assertNull(cache.compute("es", mappingToNull), "mapping to null returns null");
      assertNull(cache.get("es"), "the key is removed");

      int cacheSizeBeforeNullValueCompute = cache.size();
      assertNull(cache.compute("eus", mappingToNull), "mapping to null returns null");
      assertNull(cache.get("eus"), "the key does not exist");
      assertEquals(cacheSizeBeforeNullValueCompute, cache.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(RuntimeException.class, "hi there", () -> cache.compute("es", mappingToException));
   }

   @Test
   public void testReplaceAll() {
      BiFunction<Object, Object, Object> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
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

   public void testCustomObjectKey() {
      CustomObjectKey ck = new APINonTxOffHeapTest.CustomObjectKey("a", 1);
      assertNull(cache.get(ck));
      cache.put(ck, "blah");
      assertEquals("blah", cache.get(ck));
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStream() throws Throwable {
      super.testLockedStream();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamWithinLockedStream() {
      super.testLockedStreamWithinLockedStream();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamFunctionalCommand() throws Throwable {
      super.testLockedStreamFunctionalCommand();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamPutAll() throws Throwable {
      super.testLockedStreamPutAll();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamPutAsync() throws Throwable {
      super.testLockedStreamPutAsync();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamCompute() throws Throwable {
      super.testLockedStreamCompute();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamComputeIfPresent() throws Throwable {
      super.testLockedStreamComputeIfPresent();
   }

   @Test(enabled = false) // ISPN-8354
   @Override
   public void testLockedStreamMerge() throws Throwable {
      super.testLockedStreamMerge();
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

   private void assertCacheIsEmpty() {
      assertCacheSize(0);
   }

   static class CustomObjectKey implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;

      final String name;
      final int value;

      CustomObjectKey(String name, int value) {
         this.name = name;
         this.value = value;
      }
   }

   static class FalseEqualsKey implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;

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
