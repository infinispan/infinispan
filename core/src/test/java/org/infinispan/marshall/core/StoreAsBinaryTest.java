package org.infinispan.marshall.core;

import static org.infinispan.test.TestingUtil.createMapEntry;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
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
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
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
@Test(groups = "functional", testName = "marshall.core.MarshalledValueTest")
public class StoreAsBinaryTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(StoreAsBinaryTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replSync.dataContainer().memory().storageType(StorageType.BINARY);

      createClusteredCaches(2, "replSync", replSync);
   }

   @Override
   @AfterClass
   protected void destroy() {
      super.destroy();
   }

   @BeforeMethod
   public void resetSerializationCounts() {
      Pojo.serializationCount = 0;
      Pojo.deserializationCount = 0;
   }

   public void testNonSerializable() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Exceptions.expectException(NotSerializableException.class, () -> cache1.put("Hello", new Object()));

      Exceptions.expectException(NotSerializableException.class, () -> cache1.put(new Object(), "Hello"));
   }

   public void testReleaseObjectValueReferences() {
      Cache<String, Object> cache1 = cache(0, "replSync");
      Cache<String, Object> cache2 = cache(1, "replSync");

      assertTrue(cache1.isEmpty());
      Pojo value = new Pojo();
      cache1.put("key", value);
      assertTrue(cache1.containsKey("key"));

      DataContainer dc1 = extractComponent(cache1, InternalDataContainer.class);

      InternalCacheEntry ice = dc1.get(boxKey(cache1, "key"));
      Object o = ice.getValue();
      assertTrue(o instanceof WrappedByteArray);
      assertEquals(value, cache1.get("key"));
      assertEquals(value, unboxValue(cache1, o));

      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, InternalDataContainer.class);
      ice = dc2.get(boxKey(cache2, "key"));
      o = ice.getValue();

      assertTrue(o instanceof WrappedByteArray);
      assertEquals(value, cache2.get("key"));
      assertEquals(value, unboxValue(cache2, o));
   }

   private Object boxKey(Cache cache, Object key) {
      EncoderCache c = (EncoderCache) cache;
      return c.keyToStorage(key);
   }

   private Object unboxKey(Cache cache, Object key) {
      EncoderCache c = (EncoderCache) cache;
      return c.keyFromStorage(key);
   }

   private Object unboxValue(Cache cache, Object value) {
      EncoderCache c = (EncoderCache) cache;
      return c.valueFromStorage(value);
   }

   public void testReleaseObjectKeyReferences() {
      Cache<Object, String> cache1 = cache(0, "replSync");
      Cache<Object, String> cache2 = cache(1, "replSync");
      Pojo key = new Pojo();
      cache1.put(key, "value");

      DataContainer dc1 = extractComponent(cache1, InternalDataContainer.class);

      Object o = dc1.keySet().iterator().next();

      assertEquals("value", cache1.get(key));


      // now on cache 2
      DataContainer dc2 = extractComponent(cache2, InternalDataContainer.class);
      o = dc2.keySet().iterator().next();
      assertEquals("value", cache2.get(key));
   }

   public void testKeySetValuesEntrySetCollectionReferences() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");

      Pojo key1 = new Pojo(1), value1 = new Pojo(11), key2 = new Pojo(2), value2 = new Pojo(22);
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

      Collection values = cache2.values();
      for (Object key : values) assertTrue(expValues.remove(key));
      assertTrue("Did not see keys " + expValues + " in iterator!", expValues.isEmpty());

      Set<Map.Entry<Object, Object>> entries = cache2.entrySet();
      for (Map.Entry entry : entries) {
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

      List<Map.Entry> entryCollection = new ArrayList<>(2);

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

      List<Map.Entry> entryCollection = new ArrayList<>(3);

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

      Set expKeyEntries = ObjectDuplicator.duplicateSet(expKeys);
      Set expValueEntries = ObjectDuplicator.duplicateSet(expValues);

      Set<Object> keys = cache(0, "replSync").keySet();
      for (Object key : keys) {
         assert expKeys.remove(key);
      }
      assert expKeys.isEmpty() : "Did not see keys " + expKeys + " in iterator!";

      Collection<Object> values = cache(0, "replSync").values();
      for (Object value : values) {
         assert expValues.remove(value);
      }
      assert expValues.isEmpty() : "Did not see keys " + expValues + " in iterator!";

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();
      for (Map.Entry entry : entries) {
         assert expKeyEntries.remove(entry.getKey());
         assert expValueEntries.remove(entry.getValue());
      }
      assert expKeyEntries.isEmpty() : "Did not see keys " + expKeyEntries + " in iterator!";
      assert expValueEntries.isEmpty() : "Did not see keys " + expValueEntries + " in iterator!";
   }

   public void testEqualsAndHashCode() throws Exception {
      StreamingMarshaller marshaller = extractGlobalMarshaller(cache(0).getCacheManager());
      Pojo pojo = new Pojo();

      WrappedBytes wb = new WrappedByteArray(marshaller.objectToByteBuffer(pojo));

      WrappedBytes wb2 = new WrappedByteArray(marshaller.objectToByteBuffer(pojo));

      assertTrue(wb2.hashCode() == wb.hashCode());
      assertEquals(wb, wb2);
   }

   public void testMarshallValueWithCustomReadObjectMethod() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");
      CustomReadObjectMethod obj = new CustomReadObjectMethod();
      cache1.put("ab-key", obj);
      assertEquals(obj, cache2.get("ab-key"));

      ObjectThatContainsACustomReadObjectMethod anotherObj = new ObjectThatContainsACustomReadObjectMethod();
      anotherObj.anObjectWithCustomReadObjectMethod = obj;
      cache1.put("cd-key", anotherObj);
      assertEquals(anotherObj, cache2.get("cd-key"));
   }

   /**
    * Run this on a separate cache as it creates and stops stores, which might affect other tests.
    */
   public void testStores() {
      ConfigurationBuilder cacheCofig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCofig.dataContainer().memory().storageType(StorageType.BINARY);
      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(cacheCofig.persistence());
      dimcs.storeName(getClass().getSimpleName());
      cacheCofig.persistence().addStore(dimcs);

      defineConfigurationOnAllManagers("replSync2", cacheCofig);
      waitForClusterToForm("replSync2");
      Cache<Object, Object> cache1 = cache(0, "replSync2");
      Cache<Object, Object> cache2 = cache(1, "replSync2");

      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assertEquals(pojo, cache2.get("key"));
   }

   public void testCallbackValues() throws Exception {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");
      MockListener l = new MockListener();
      cache1.addListener(l);
      try {
         Pojo pojo = new Pojo();
         cache1.put("key", pojo);
         assertTrue("received " + l.newValue.getClass().getName(), l.newValue instanceof Pojo);
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
         Pojo pojo = new Pojo();
         cache1.put("key", pojo);
         assertTrue(l.newValue instanceof Pojo);
      } finally {
         cache2.removeListener(l);
      }
   }

   public void testEvictWithMarshalledValueKey() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");
      Pojo pojo = new Pojo();
      cache1.put(pojo, pojo);
      cache1.evict(pojo);
      assertTrue(!cache1.containsKey(pojo));
   }

   public void testModificationsOnSameCustomKey() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      Cache<Object, Object> cache2 = cache(1, "replSync");

      Pojo key1 = new Pojo();
      log.trace("First put");
      cache1.put(key1, "1");

      log.trace("Second put");
      Pojo key2 = new Pojo();
      assertEquals("1", cache2.put(key2, "2"));
   }

   public void testReturnValueDeserialization() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Pojo v1 = new Pojo(1);
      cache1.put("1", v1);
      Pojo previous = (Pojo) cache1.put("1", new Pojo(2));
      assertEquals(v1, previous);
   }

   public void testGetCacheEntryWithFlag() {
      Cache<Object, Object> cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Pojo key1 = new Pojo();
      cache1.put(key1, "1");

      assertEquals("1", cache1.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).getCacheEntry(key1).getValue());
   }

   @Listener
   public static class MockListener {
      Object newValue;

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent e) {
         if (!e.isPre()) newValue = e.getValue();
      }

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent e) {
         if (!e.isPre()) newValue = e.getValue();
      }
   }

   public static class Pojo implements Externalizable, ExternalPojo {
      public int i;
      static int serializationCount, deserializationCount;
      final Log log = LogFactory.getLog(Pojo.class);
      private static final long serialVersionUID = -2888014339659501395L;

      Pojo(int i) {
         this.i = i;
      }

      public Pojo() {
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         return i == pojo.i;
      }

      @Override
      public int hashCode() {
         return i;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         int serCount = updateSerializationCount();
         log.trace("serializationCount=" + serCount);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         int deserCount = updateDeserializationCount();
         log.trace("deserializationCount=" + deserCount);
      }

      int updateSerializationCount() {
         return ++serializationCount;
      }

      int updateDeserializationCount() {
         return ++deserializationCount;
      }

      @Override
      public String toString() {
         return "Pojo{" +
               "i=" + i +
               '}';
      }
   }

   public static class ObjectThatContainsACustomReadObjectMethod implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;
      //      Integer id;
      CustomReadObjectMethod anObjectWithCustomReadObjectMethod;
      Integer balance;
//      String branch;

      @Override
      public boolean equals(Object obj) {
         if (obj == this)
            return true;
         if (!(obj instanceof ObjectThatContainsACustomReadObjectMethod))
            return false;
         ObjectThatContainsACustomReadObjectMethod acct = (ObjectThatContainsACustomReadObjectMethod) obj;
//         if (!safeEquals(id, acct.id))
//            return false;
//         if (!safeEquals(branch, acct.branch))
//            return false;
         return safeEquals(balance, acct.balance) && safeEquals(anObjectWithCustomReadObjectMethod, acct.anObjectWithCustomReadObjectMethod);
      }

      @Override
      public int hashCode() {
         int result = 17;
//         result = result * 31 + safeHashCode(id);
//         result = result * 31 + safeHashCode(branch);
         result = result * 31 + safeHashCode(balance);
         result = result * 31 + safeHashCode(anObjectWithCustomReadObjectMethod);
         return result;
      }

      private static int safeHashCode(Object obj) {
         return obj == null ? 0 : obj.hashCode();
      }

      private static boolean safeEquals(Object a, Object b) {
         return (a == b || (a != null && a.equals(b)));
      }
   }

   public static class CustomReadObjectMethod implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;
      String lastName;
      String ssn;
      transient boolean deserialized;

      CustomReadObjectMethod() {
         this("Zamarreno", "234-567-8901");
      }

      CustomReadObjectMethod(String lastName, String ssn) {
         this.lastName = lastName;
         this.ssn = ssn;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj == this) return true;
         if (!(obj instanceof CustomReadObjectMethod)) return false;
         CustomReadObjectMethod pk = (CustomReadObjectMethod) obj;
         return lastName.equals(pk.lastName) && ssn.equals(pk.ssn);
      }

      @Override
      public int hashCode() {
         int result = 17;
         result = result * 31 + lastName.hashCode();
         result = result * 31 + ssn.hashCode();
         return result;
      }

      private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
         ois.defaultReadObject();
         deserialized = true;
      }
   }
}
