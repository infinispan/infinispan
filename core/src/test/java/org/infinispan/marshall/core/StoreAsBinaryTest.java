package org.infinispan.marshall.core;

import static org.infinispan.test.TestingUtil.createMapEntry;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.ObjectDuplicator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.CountMarshallingPojo;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests implicit marshalled values
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "marshall.core.StoreAsBinaryTest")
public class StoreAsBinaryTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(StoreAsBinaryTest.class);
   private static final String POJO_NAME = StoreAsBinaryTest.class.getName();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replSync.memory().storageType(StorageType.BINARY);
      createClusteredCaches(2, "replSync", TestDataSCI.INSTANCE, replSync);
   }

   @Override
   @AfterClass
   protected void destroy() {
      super.destroy();
   }

   @BeforeMethod
   public void resetSerializationCounts() {
      CountMarshallingPojo.reset(POJO_NAME);
   }

   public void testNonMarshallable() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Exceptions.expectException(MarshallingException.class, () -> cache1.put("Hello", new Object()));

      Exceptions.expectException(MarshallingException.class, () -> cache1.put(new Object(), "Hello"));
   }

   public void testReleaseObjectValueReferences() {
      Cache<String, CountMarshallingPojo> cache1 = cache(0, "replSync");
      Cache<String, CountMarshallingPojo> cache2 = cache(1, "replSync");

      assertTrue(cache1.isEmpty());
      CountMarshallingPojo value = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put("key", value);
      assertTrue(cache1.containsKey("key"));

      DataContainer<?, ?> dc1 = extractComponent(cache1, InternalDataContainer.class);

      InternalCacheEntry<?, ?> ice = dc1.peek(boxKey(cache1, "key"));
      Object o = ice.getValue();
      assertTrue(o instanceof WrappedByteArray);
      assertEquals(value, cache1.get("key"));
      assertEquals(value, unboxValue(cache1, o));

      // now on cache 2
      DataContainer<?, ?> dc2 = TestingUtil.extractComponent(cache2, InternalDataContainer.class);
      ice = dc2.peek(boxKey(cache2, "key"));
      o = ice.getValue();

      assertTrue(o instanceof WrappedByteArray);
      assertEquals(value, cache2.get("key"));
      assertEquals(value, unboxValue(cache2, o));
   }

   private Object boxKey(Cache<?, ?> cache, Object key) {
      EncoderCache<?, ?> c = (EncoderCache<?, ?>) cache;
      return c.keyToStorage(key);
   }

   private Object unboxKey(Cache<?, ?> cache, Object key) {
      EncoderCache<?, ?> c = (EncoderCache<?, ?>) cache;
      return c.keyFromStorage(key);
   }

   private Object unboxValue(Cache<?, ?> cache, Object value) {
      EncoderCache<?, ?> c = (EncoderCache<?, ?>) cache;
      return c.valueFromStorage(value);
   }

   public void testReleaseObjectKeyReferences() {
      Cache<Object, String> cache1 = cache(0, "replSync");
      Cache<Object, String> cache2 = cache(1, "replSync");
      CountMarshallingPojo key = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put(key, "value");

      DataContainer<Object, String> dc1 = extractComponent(cache1, InternalDataContainer.class);

      Object firstKeyStorage = dc1.iterator().next().getKey();
      Object firstKey = cache1.getAdvancedCache().getKeyDataConversion().fromStorage(firstKeyStorage);
      assertEquals(key, firstKey);
      assertEquals("value", cache1.get(key));


      // now on cache 2
      DataContainer<Object, String> dc2 = extractComponent(cache2, InternalDataContainer.class);
      firstKeyStorage = dc2.iterator().next().getKey();
      firstKey = cache1.getAdvancedCache().getKeyDataConversion().fromStorage(firstKeyStorage);
      assertEquals(key, firstKey);
      assertEquals("value", cache2.get(key));
   }

   public void testKeySetValuesEntrySetCollectionReferences() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");

      CountMarshallingPojo key1 = new CountMarshallingPojo(POJO_NAME, 1);
      CountMarshallingPojo value1 = new CountMarshallingPojo(POJO_NAME, 11);
      CountMarshallingPojo key2 = new CountMarshallingPojo(POJO_NAME, 2);
      CountMarshallingPojo value2 = new CountMarshallingPojo(POJO_NAME, 22);
      String key3 = "3", value3 = "three";
      cache1.put(key1, value1);
      cache1.put(key2, value2);
      cache1.put(key3, value3);

      Set<Object> expKeys = new HashSet<>();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);

      Set<Object> expValues = new HashSet<>();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);

      Set<Object> expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set<Object> expValueEntries = ObjectDuplicator.duplicateSet(expValues);

      Set<Object> keys = cache2.keySet();
      for (Object key : keys) assertTrue(expKeys.remove(key));
      assertTrue("Did not see keys " + expKeys + " in iterator!", expKeys.isEmpty());

      Collection<Object> values = cache2.values();
      for (Object key : values) assertTrue(expValues.remove(key));
      assertTrue("Did not see keys " + expValues + " in iterator!", expValues.isEmpty());

      Set<Map.Entry<Object, Object>> entries = cache2.entrySet();
      for (Map.Entry<Object, Object> entry : entries) {
         assertTrue(expKeyEntries.remove(entry.getKey()));
         assertTrue(expValueEntries.remove(entry.getValue()));
      }
      assertTrue("Did not see keys " + expKeyEntries + " in iterator!", expKeyEntries.isEmpty());
      assertTrue("Did not see keys " + expValueEntries + " in iterator!", expValueEntries.isEmpty());
   }

   public void testUnsupportedKeyValueCollectionOperations() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Object> keys = cache(0, "replSync").keySet();
      Collection<Object> values = cache(0, "replSync").values();
      //noinspection unchecked
      Collection<Object>[] collections = new Collection[]{keys, values};

      Object newObj = "foo";
      List<Object> newObjCol = new ArrayList<>();
      newObjCol.add(newObj);
      for (Collection<Object> col : collections) {
         Exceptions.expectException(UnsupportedOperationException.class, () -> col.add(newObj));
         Exceptions.expectException(UnsupportedOperationException.class, () -> col.addAll(newObjCol));
      }
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testAddMethodsForEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();

      entries.add(createMapEntry("4", "four"));
   }

   public void testRemoveMethodOfKeyValueEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Object> keys = cache(0, "replSync").keySet();
      keys.remove(key1);

      assertEquals(2, cache(0, "replSync").size());

      Collection<Object> values = cache(0, "replSync").values();
      values.remove(value2);

      assertEquals(1, cache(0, "replSync").size());

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      entries.remove(TestingUtil.<Object, Object>createMapEntry(key3, value3));

      assertEquals(0, cache(0, "replSync").size());
   }

   public void testClearMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Object> keys = cache(0, "replSync").keySet();
      keys.clear();

      assertEquals(0, cache(0, "replSync").size());
   }

   public void testClearMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Collection<Object> values = cache(0, "replSync").values();
      values.clear();

      assertEquals(0, cache(0, "replSync").size());
   }

   public void testClearMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      entries.clear();

      assertEquals(0, cache(0, "replSync").size());
   }

   public void testRemoveAllMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<String> keyCollection = new ArrayList<>(2);

      keyCollection.add(key2);
      keyCollection.add(key3);

      Collection<Object> keys = cache(0, "replSync").keySet();
      keys.removeAll(keyCollection);

      assertEquals(1, cache(0, "replSync").size());
   }

   public void testRemoveAllMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<String> valueCollection = new ArrayList<>(2);

      valueCollection.add(value1);
      valueCollection.add(value2);

      Collection<Object> values = cache(0, "replSync").values();
      values.removeAll(valueCollection);

      assertEquals(1, cache(0, "replSync").size());
   }

   public void testRemoveAllMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<Map.Entry<Object, Object>> entryCollection = new ArrayList<>(2);

      entryCollection.add(createMapEntry(key1, value1));
      entryCollection.add(createMapEntry(key3, value3));

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      entries.removeAll(entryCollection);

      assertEquals(1, cache(0, "replSync").size());
   }

   public void testRetainAllMethodOfKeyCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<String> keyCollection = new ArrayList<>(2);

      keyCollection.add(key2);
      keyCollection.add(key3);
      keyCollection.add("6");

      Collection<Object> keys = cache(0, "replSync").keySet();
      keys.retainAll(keyCollection);

      assertEquals(2, cache(0, "replSync").size());
   }

   public void testRetainAllMethodOfValuesCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<String> valueCollection = new ArrayList<>(2);

      valueCollection.add(value1);
      valueCollection.add(value2);
      valueCollection.add("5");

      Collection<Object> values = cache(0, "replSync").values();
      values.retainAll(valueCollection);

      assertEquals(2, cache(0, "replSync").size());
   }

   public void testRetainAllMethodOfEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      List<Map.Entry<Object, Object>> entryCollection = new ArrayList<>(3);

      entryCollection.add(createMapEntry(key1, value1));
      entryCollection.add(createMapEntry(key3, value3));
      entryCollection.add(createMapEntry("4", "5"));

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      entries.retainAll(entryCollection);

      assertEquals(2, cache(0, "replSync").size());
   }

   public void testEntrySetValueFromEntryCollections() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      String newString = "new-value";

      for (Map.Entry<Object, Object> entry : entries) {
         entry.setValue(newString);
      }

      assertEquals(3, cache(0, "replSync").size());

      assertEquals(newString, cache(0, "replSync").get(key1));
      assertEquals(newString, cache(0, "replSync").get(key2));
      assertEquals(newString, cache(0, "replSync").get(key3));
   }

   public void testKeyValueEntryCollections() {
      String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);
      assert 3 == cache(0, "replSync").size() && 3 == cache(0, "replSync").keySet().size() && 3 == cache(0, "replSync").values().size() && 3 == cache(0, "replSync").entrySet().size();

      Set<Object> expKeys = new HashSet<>();
      expKeys.add(key1);
      expKeys.add(key2);
      expKeys.add(key3);

      Set<Object> expValues = new HashSet<>();
      expValues.add(value1);
      expValues.add(value2);
      expValues.add(value3);

      Set<Object> expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set<Object> expValueEntries = ObjectDuplicator.duplicateSet(expValues);

      Set<Object> keys = cache(0, "replSync").keySet();
      for (Object key : keys) {
         assertTrue(expKeys.remove(key));
      }
      assertTrue("Did not see keys " + expKeys + " in iterator!", expKeys.isEmpty());

      Collection<Object> values = cache(0, "replSync").values();
      for (Object value : values) {
         assertTrue(expValues.remove(value));
      }
      assertTrue("Did not see keys " + expValues + " in iterator!", expValues.isEmpty());

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      for (Map.Entry<Object, Object> entry : entries) {
         assertTrue(expKeyEntries.remove(entry.getKey()));
         assertTrue(expValueEntries.remove(entry.getValue()));
      }
      assertTrue("Did not see keys " + expKeyEntries + " in iterator!", expKeyEntries.isEmpty());
      assertTrue("Did not see keys " + expValueEntries + " in iterator!", expValueEntries.isEmpty());
   }

   public void testEqualsAndHashCode() throws Exception {
      Marshaller marshaller = extractGlobalMarshaller(manager(0));
      CountMarshallingPojo pojo = new CountMarshallingPojo(POJO_NAME, 1);
      WrappedBytes wb = new WrappedByteArray(marshaller.objectToByteBuffer(pojo));
      WrappedBytes wb2 = new WrappedByteArray(marshaller.objectToByteBuffer(pojo));
      assertEquals(wb2.hashCode(), wb.hashCode());
      assertEquals(wb, wb2);
   }

   public void testComputeIfAbsentMethods() {
      Cache<Object, Object> cache = cache(0, "replSync");
      cache.computeIfAbsent("1", k -> k + "1", -1, TimeUnit.NANOSECONDS);
      assertEquals(1, cache.size());

      cache.computeIfAbsent("2", k -> k + "2", -1, TimeUnit.NANOSECONDS, -1, TimeUnit.NANOSECONDS);
      assertEquals(2, cache.size());
   }

   /**
    * Run this on a separate cache as it creates and stops stores, which might affect other tests.
    */
   public void testStores() {
      ConfigurationBuilder cacheCofig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCofig.memory().storageType(StorageType.BINARY);
      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(cacheCofig.persistence());
      dimcs.storeName(getClass().getSimpleName());
      cacheCofig.persistence().addStore(dimcs);

      defineConfigurationOnAllManagers("replSync2", cacheCofig);
      waitForClusterToForm("replSync2");
      Cache<Object, Object> cache1 = cache(0, "replSync2");
      Cache<Object, Object> cache2 = cache(1, "replSync2");

      CountMarshallingPojo pojo = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put("key", pojo);

      assertEquals(pojo, cache2.get("key"));
   }

   public void testCallbackValues() throws Exception {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");
      MockListener l = new MockListener();
      cache1.addListener(l);
      try {
         CountMarshallingPojo pojo = new CountMarshallingPojo(POJO_NAME, 1);
         cache1.put("key", pojo);
         assertTrue("received " + l.newValue.getClass().getName(), l.newValue instanceof CountMarshallingPojo);
      } finally {
         cache1.removeListener(l);
      }
   }

   public void testRemoteCallbackValues() throws Exception {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");
      MockListener l = new MockListener();
      cache2.addListener(l);
      try {
         CountMarshallingPojo pojo = new CountMarshallingPojo(POJO_NAME, 1);
         cache1.put("key", pojo);
         assertTrue(l.newValue instanceof CountMarshallingPojo);
      } finally {
         cache2.removeListener(l);
      }
   }

   public void testEvictWithMarshalledValueKey() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");
      CountMarshallingPojo pojo = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put(pojo, pojo);
      cache1.evict(pojo);
      assertFalse(cache1.containsKey(pojo));
   }

   public void testModificationsOnSameCustomKey() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");

      CountMarshallingPojo key1 = new CountMarshallingPojo(POJO_NAME, 1);
      log.trace("First put");
      cache1.put(key1, "1");

      log.trace("Second put");
      CountMarshallingPojo key2 = new CountMarshallingPojo(POJO_NAME, 1);
      assertEquals("1", cache2.put(key2, "2"));
   }

   public void testReturnValueDeserialization() {
      Cache<String, CountMarshallingPojo> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      CountMarshallingPojo v1 = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put("1", v1);
      CountMarshallingPojo previous = cache1.put("1", new CountMarshallingPojo(POJO_NAME, 2));
      assertEquals(v1, previous);
   }

   public void testGetCacheEntryWithFlag() {
      Cache<CountMarshallingPojo, String> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      CountMarshallingPojo key1 = new CountMarshallingPojo(POJO_NAME, 1);
      cache1.put(key1, "1");

      assertEquals("1", cache1.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).getCacheEntry(key1).getValue());
   }

   @Listener
   public static class MockListener {
      Object newValue;

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<Object, Object> e) {
         if (!e.isPre()) newValue = e.getValue();
      }

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<Object, Object> e) {
         if (!e.isPre()) newValue = e.getValue();
      }
   }
}
