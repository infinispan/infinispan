package org.infinispan.marshall.core;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.util.ObjectDuplicator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.MarshalledValueInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Tests implicit marshalled values
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Test(groups = "functional", testName = "marshall.core.MarshalledValueTest")
public class MarshalledValueTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(MarshalledValueTest.class);
   private MarshalledValueListenerInterceptor mvli;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replSync.dataContainer().storeAsBinary().enable();

      createClusteredCaches(2, "replSync", replSync);

      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);

      // Prime the IsMarshallableInterceptor so that it doesn't trigger additional serialization during tests
      Pojo key = new Pojo(-1);
      cache1.get(key);
      assertSerializationCounts(1, 0);
      cache2.get(key);
      assertSerializationCounts(2, 0);
   }

   private void assertMarshalledValueInterceptorPresent(Cache c) {
      InterceptorChain ic1 = TestingUtil.extractComponent(c, InterceptorChain.class);
      assertTrue(ic1.containsInterceptorType(MarshalledValueInterceptor.class));
   }

   @BeforeMethod
   public void addMarshalledValueInterceptor() {
      Cache cache1;
      cache1 = cache(0, "replSync");
      cache(1, "replSync");
      InterceptorChain chain = TestingUtil.extractComponent(cache1, InterceptorChain.class);
      chain.removeInterceptor(MarshalledValueListenerInterceptor.class);
      mvli = new MarshalledValueListenerInterceptor();
      chain.addInterceptorAfter(mvli, MarshalledValueInterceptor.class);
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

   private void assertSerialized(MarshalledValue mv) {
      assertTrue("Should be serialized", mv.getRaw() != null);
   }

   private void assertSerializationCounts(int serializationCount, int deserializationCount) {
      assertEquals("Serialization count mismatch", serializationCount, Pojo.serializationCount);
      assertEquals("Deserialization count mismatch", deserializationCount, Pojo.deserializationCount);
   }

   public void testNonSerializable() {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
      try {
         cache1.put("Hello", new Object());
         fail("Should have failed");
      }
      catch (CacheException expected) {

      }

      assertTrue("Call should not have gone beyond the MarshalledValueInterceptor", mvli.invocationCount == 0);

      try {
         cache1.put(new Object(), "Hello");
         fail("Should have failed");
      }
      catch (CacheException expected) {

      }

      assertTrue("Call should not have gone beyond the MarshalledValueInterceptor", mvli.invocationCount == 0);
   }

   public void testReleaseObjectValueReferences() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      assertTrue(cache1.isEmpty());
      Pojo value = new Pojo();
      log.trace(TestingUtil.extractComponent(cache1, InterceptorChain.class).toString());
      cache1.put("key", value);
      assertTrue(cache1.containsKey("key"));
      assertSerializationCounts(1, 1);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      InternalCacheEntry ice = dc1.get("key");
      Object o = ice.getValue();
      assertTrue(o instanceof MarshalledValue);
      MarshalledValue mv = (MarshalledValue) o;
      assertEquals(value, cache1.get("key"));
      assertSerializationCounts(1, 2);
      assertSerialized(mv);

      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      ice = dc2.get("key");
      o = ice.getValue();
      assertTrue(o instanceof MarshalledValue);
      mv = (MarshalledValue) o;
      assertSerialized(mv); // this proves that unmarshalling on the recipient cache instance is lazy

      assertEquals(value, cache2.get("key"));
      assertSerializationCounts(1, 3);
      assertSerialized(mv);
   }

   public void testReleaseObjectKeyReferences() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      Pojo key = new Pojo();
      cache1.put(key, "value");

      assertSerializationCounts(1, 0);

      DataContainer dc1 = TestingUtil.extractComponent(cache1, DataContainer.class);

      Object o = dc1.keySet().iterator().next();
      assertTrue(o instanceof MarshalledValue);
      MarshalledValue mv = (MarshalledValue) o;
      assertSerialized(mv);

      assertEquals("value", cache1.get(key));
      // Key is non-primitive type, so eargerly serialized
      assertSerializationCounts(2, 0);
      assertSerialized(mv);


      // now on cache 2
      DataContainer dc2 = TestingUtil.extractComponent(cache2, DataContainer.class);
      o = dc2.keySet().iterator().next();
      assertTrue(o instanceof MarshalledValue);
      mv = (MarshalledValue) o;
      assertSerialized(mv);
      assertEquals("value", cache2.get(key));
      assertSerializationCounts(3, 0);
      assertSerialized(mv);
   }

   public void testKeySetValuesEntrySetCollectionReferences() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      Pojo key1 = new Pojo(1), value1 = new Pojo(11), key2 = new Pojo(2), value2 = new Pojo(22);
      String key3 = "3", value3 = "three";
      cache1.put(key1, value1);
      cache1.put(key2, value2);
      cache1.put(key3, value3);

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

      Set keys = cache2.keySet();
      for (Object key : keys) assertTrue(expKeys.remove(key));
      assertTrue("Did not see keys " + expKeys + " in iterator!", expKeys.isEmpty());

      Collection values = cache2.values();
      for (Object key : values) assertTrue(expValues.remove(key));
      assertTrue("Did not see keys " + expValues + " in iterator!", expValues.isEmpty());

      Set<Map.Entry> entries = cache2.entrySet();
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
      Collection[] collections = new Collection[]{keys, values};

      Object newObj = new Object();
      List newObjCol = new ArrayList();
      newObjCol.add(newObj);
      for (Collection col : collections) {
         try {
            col.add(newObj);
            fail("Should have thrown a UnsupportedOperationException");
         } catch (UnsupportedOperationException uoe) {
         } catch (ClassCastException e) {
            // Ignore class cast in expired filtered set because
            // you cannot really add an Object type instance.
         }
         try {
            col.addAll(newObjCol);
            fail("Should have thrown a UnsupportedOperationException");
         } catch (UnsupportedOperationException uoe) {
         }
      }
   }

   public void testAddMethodsForEntryCollection() {
      final String key1 = "1", value1 = "one", key2 = "2", value2 = "two", key3 = "3", value3 = "three";
      Map<String, String> m = new HashMap<>();
      m.put(key1, value1);
      m.put(key2, value2);
      m.put(key3, value3);
      cache(0, "replSync").putAll(m);

      Set<Map.Entry<Object, Object>> entries = cache(0, "replSync").entrySet();

      Map.Entry entry = new ImmortalCacheEntry("4", "four");
      entries.add(entry);

      assertEquals(4, cache(0, "replSync").size());

      List<Map.Entry<Object, Object>> entryCollection = new ArrayList<>(2);

      entryCollection.add(new ImmortalCacheEntry("5", "five"));
      entryCollection.add(new ImmortalCacheEntry("6", "six"));

      entries.addAll(entryCollection);

      assertEquals(6, cache(0, "replSync").size());
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
      entries.remove(new ImmortalCacheEntry(key3, value3));

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

      entryCollection.add(new ImmortalCacheEntry(key1, value1));
      entryCollection.add(new ImmortalCacheEntry(key3, value3));

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

      entryCollection.add(new ImmortalCacheEntry(key1, value1));
      entryCollection.add(new ImmortalCacheEntry(key3, value3));
      entryCollection.add(new ImmortalCacheEntry("4", "5"));

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

      for (Map.Entry entry : entries) {
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
      Pojo pojo = new Pojo();
      MarshalledValue mv = new MarshalledValue(pojo, extractCacheMarshaller(cache(0)));
      int oldHashCode = mv.hashCode();
      assertSerialized(mv);
      assertTrue(oldHashCode == mv.hashCode());

      MarshalledValue mv2 = new MarshalledValue(pojo, extractCacheMarshaller(cache(0)));
      assertSerialized(mv);

      assertTrue(mv2.hashCode() == oldHashCode);
      assertEquals(mv, mv2);
   }

   public void testMarshallValueWithCustomReadObjectMethod() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
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
      cacheCofig.dataContainer().storeAsBinary().enable();
      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(cacheCofig.persistence());
      dimcs.storeName(getClass().getSimpleName());
      cacheCofig.persistence().addStore(dimcs);

      defineConfigurationOnAllManagers("replSync2", cacheCofig);
      waitForClusterToForm("replSync2");
      Cache<Object, Object> cache1 = cache(0, "replSync2");
      Cache<Object, Object> cache2 = cache(1, "replSync2");

      Pojo pojo = new Pojo();
      cache1.put("key", pojo);

      assertMarshalledValueInterceptorPresent(cache1);
      assertMarshalledValueInterceptorPresent(cache2);
      assertSerializationCounts(1, 0);

      cache2.get("key");

      assertSerializationCounts(1, 1);
   }

   public void testCallbackValues() throws Exception {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
      MockListener l = new MockListener();
      cache1.addListener(l);
      try {
         Pojo pojo = new Pojo();
         cache1.put("key", pojo);
         assertTrue("recieved " + l.newValue.getClass().getName(), l.newValue instanceof Pojo);
         assertSerializationCounts(1, 1);
      } finally {
         cache1.removeListener(l);
      }
   }

   public void testRemoteCallbackValues() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      MockListener l = new MockListener();
      cache2.addListener(l);
      try {
         Pojo pojo = new Pojo();
         // Mock listener will force deserialization on transport thread. Ignore this by setting b to false.
         pojo.b = false;
         cache1.put("key", pojo);
         assertTrue(l.newValue instanceof Pojo);
         assertSerializationCounts(1, 1);
      } finally {
         cache2.removeListener(l);
      }
   }

   public void testEvictWithMarshalledValueKey() {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");
      Pojo pojo = new Pojo();
      cache1.put(pojo, pojo);
      cache1.evict(pojo);
      assertTrue(!cache1.containsKey(pojo));
   }

   public void testModificationsOnSameCustomKey() {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");

      Pojo key1 = new Pojo();
      log.trace("First put");
      cache1.put(key1, "1");
      // 1 serialization on cache1 (the primary), when replicating the command to cache2 (the backup)
      assertSerializationCounts(1, 0);

      log.trace("Second put");
      Pojo key2 = new Pojo();
      cache2.put(key2, "2");
      // 1 serialization on cache2 for key2, when replicating the command to cache1 (the primary)
      // 1 serialization on cache1 for key1
      assertSerializationCounts(2, 0);
   }

   public void testReturnValueDeserialization() {
      Cache cache1 = cache(0, "replSync");
      cache(1, "replSync");

      Pojo v1 = new Pojo(1);
      cache1.put("1", v1);
      Pojo previous = (Pojo) cache1.put("1", new Pojo(2));
      assertEquals(v1, previous);
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

   class MarshalledValueListenerInterceptor extends CommandInterceptor {
      int invocationCount = 0;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         invocationCount++;
         Object retval = invokeNextInterceptor(ctx, command);
         return retval;
      }

   }

   public static class Pojo implements Externalizable {
      public int i;
      boolean b = true;
      static int serializationCount, deserializationCount;
      final Log log = LogFactory.getLog(Pojo.class);
      private static final long serialVersionUID = -2888014339659501395L;

      public Pojo(int i) {
         this.i = i;
      }

      public Pojo() {
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (b != pojo.b) return false;
         if (i != pojo.i) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result;
         result = i;
         result = 31 * result + (b ? 1 : 0);
         return result;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeInt(i);
         out.writeBoolean(b);
         int serCount = updateSerializationCount();
         log.trace("serializationCount=" + serCount);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         i = in.readInt();
         b = in.readBoolean();
         if (b) {
            // TODO: Find a better way to make sure a transport (JGroups) thread is not attempting to deserialize stuff
            assertTrue("Transport (JGroups) thread is trying to deserialize stuff!!", !Thread.currentThread().getName().startsWith("OOB"));
         }
         int deserCount = updateDeserializationCount();
         log.trace("deserializationCount=" + deserCount);
      }

      public int updateSerializationCount() {
         return ++serializationCount;
      }

      public int updateDeserializationCount() {
         return ++deserializationCount;
      }
   }

   public static class ObjectThatContainsACustomReadObjectMethod implements Serializable {
      private static final long serialVersionUID = 1L;
//      Integer id;
      public CustomReadObjectMethod anObjectWithCustomReadObjectMethod;
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
         if (!safeEquals(balance, acct.balance))
            return false;
         if (!safeEquals(anObjectWithCustomReadObjectMethod, acct.anObjectWithCustomReadObjectMethod))
            return false;
         return true;
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

   public static class CustomReadObjectMethod implements Serializable {
      private static final long serialVersionUID = 1L;
      String lastName;
      String ssn;
      transient boolean deserialized;

      public CustomReadObjectMethod( ) {
         this("Zamarreno", "234-567-8901");
      }

      public CustomReadObjectMethod(String lastName, String ssn) {
         this.lastName = lastName;
         this.ssn = ssn;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj == this) return true;
         if (!(obj instanceof CustomReadObjectMethod)) return false;
         CustomReadObjectMethod pk = (CustomReadObjectMethod) obj;
         if (!lastName.equals(pk.lastName)) return false;
         if (!ssn.equals(pk.ssn)) return false;
         return true;
      }

      @Override
      public int hashCode( ) {
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
