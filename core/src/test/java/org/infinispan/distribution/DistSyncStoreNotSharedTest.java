package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.marshaller;
import static org.testng.AssertJUnit.*;

/**
 * DistSyncSharedTest.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.DistSyncStoreNotSharedTest")
@CleanupAfterMethod
public class DistSyncStoreNotSharedTest extends BaseDistStoreTest<Object, String> {

   private static final String k1 = "1", v1 = "one", k2 = "2", v2 = "two", k3 = "3", v3 = "three", k4 = "4", v4 = "four";
   private static final String[] keys = new String[]{k1, k2, k3, k4};

   public DistSyncStoreNotSharedTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      shared = false;
   }

   public void testPutFromNonOwner(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheLoader nonOwnerLoader = TestingUtil.getFirstLoader(nonOwner);
      CacheLoader ownerLoader = TestingUtil.getFirstLoader(owner);
      assertFalse(nonOwnerLoader.contains(key));
      assertFalse(ownerLoader.contains(key));
      Object retval = nonOwner.put(key, value);
      assertFalse(nonOwnerLoader.contains(key));
      assertTrue(ownerLoader.contains(key));
      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }
   
   public void testGetFromNonOwnerWithFlags(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheLoader ownerLoader = TestingUtil.getFirstLoader(owner);
      owner.put(key, value);
      assertEquals(value, ownerLoader.load(key).getValue());
      owner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();
      assertEquals(value, ownerLoader.load(key).getValue());
      assertNull(owner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get(key));
      assertNull(nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get(key));
      assertEquals(value, nonOwner.get(key));
      // need to do the get() on all the owners first to load the values, otherwise assertOwnershipAndNonOwnership might fail
      assertOnAllCaches(key, value);
      assertOwnershipAndNonOwnership(key, true);
   }
   
   public void testAsyncGetCleansContextFlags(Method m) throws Exception {
      String key = k(m), value = "value2";

      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      owner.put(key, value);

      owner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();

      Future<String> async = nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).getAsync(key);
      assertNotNull(async);
      assertNull(async.get());

      async = nonOwner.getAdvancedCache().getAsync(key);
      assertNotNull(async);
      String returnedValue = async.get();
      assertEquals(value, returnedValue);
   }

   public void testPutFromNonOwnerWithFlags(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheLoader nonOwnerStore = TestingUtil.getFirstLoader(nonOwner);
      CacheLoader ownerStore = TestingUtil.getFirstLoader(owner);
      assertFalse(nonOwnerStore.contains(key));
      assertFalse(ownerStore.contains(key));
      Object retval = nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).put(key, value);
      assertFalse(nonOwnerStore.contains(key));
      assertFalse(ownerStore.contains(key));
      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }

   public void testPutFromOwner(Method m) throws Exception {
      String key = k(m), value = "value3";
      getOwners(key)[0].put(key, value);
      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assertTrue(store.contains(key));
         } else {
            assertIsNotInL1(c, key);
            assertFalse(store.contains(key));
         }
      }
   }

   public void testPutForStateTransfer() throws Exception {
      MagicKey k1 = new MagicKey(c1, c2);
      CacheLoader store2 = TestingUtil.getFirstLoader(c2);

      c2.put(k1, v1);
      assertTrue(store2.contains(k1));
      assertEquals(v1, store2.load(k1).getValue());

      c2.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put(k1, v2);
      assertEquals(v2, store2.load(k1).getValue());
   }

   public void testPutAll() throws Exception {

      c1.putAll(makePutAllTestData());

      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         for (String key : keys) {
            if (isOwner(c, key)) {
               assertIsInContainerImmortal(c, key);
               assertTrue(store.contains(key));
            } else {
               assertFalse(store.contains(key));
            }
         }
      }
   }

   public void testPutAllWithFlags() throws Exception {
      Map<String, String> data = makePutAllTestData();
      c1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).putAll(data);

      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         for (String key : keys) {
            assertFalse(store.contains(key));
            if (isOwner(c, key)) {
               assertIsInContainerImmortal(c, key);
            }
         }
      }
   }

   public void testRemoveFromNonOwner() throws Exception {
      String key = "k1", value = "value";
      initAndTest();

      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assertEquals(value, store.load(key).getValue());
         } else {
            assertFalse(store.contains(key));
         }
      }

      Object retval = getFirstNonOwner(key).remove(key);
      if (testRetVals) assert "value".equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         assertFalse(store.contains(key));
      }
   }
   
   public void testRemoveFromNonOwnerWithFlags() throws Exception {
      String key = "k1", value = "value";
      initAndTest();
      Object retval = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).remove(key);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         if (isOwner(c, key)) {
            CacheLoader store = TestingUtil.getFirstLoader(c);
            assertTrue(store.contains(key));
         }
      }
   }

   public void testReplaceFromNonOwner() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();

      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assertEquals(value, store.load(key).getValue());
         } else {
            assertFalse(store.contains(key));
         }
      }

      Object retval = getFirstNonOwner(key).replace(key, value2);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assertEquals(value2, c.get(key));
            assertEquals(value2, store.load(key).getValue());
         } else {
            assertFalse(store.contains(key));
         }
      }
   }
   
   public void testReplaceFromNonOwnerWithFlag() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();
      Object retval = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).replace(key, value2);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assertEquals(value2, c.get(key));
            assertEquals(value, store.load(key).getValue());
         } else {
            assertFalse(store.contains(key));
         }
      }
   }
   
   public void testAtomicReplaceFromNonOwner() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();
      boolean replaced = getFirstNonOwner(key).replace(key, value2, value);
      assertFalse(replaced);
      replaced = getFirstNonOwner(key).replace(key, value, value2);
      assertTrue(replaced);
      for (Cache<Object, String> c : caches) {
         assertEquals(value2, c.get(key));
         if (isOwner(c, key)) {
            CacheLoader store = TestingUtil.getFirstLoader(c);
            assertTrue(store.contains(key));
            assertEquals(value2, store.load(key).getValue());
         }
      }
   }
   
   public void testAtomicReplaceFromNonOwnerWithFlag() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();
      boolean replaced = getFirstNonOwner(key).replace(key, value2, value);
      assertFalse(replaced);
      replaced = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).replace(key, value, value2);
      assertTrue(replaced);
      for (Cache<Object, String> c : caches) {
         assertEquals(value2, c.get(key));
         if (isOwner(c, key)) {
            CacheLoader store = TestingUtil.getFirstLoader(c);
            assertTrue(store.contains(key));
            assertEquals(value, store.load(key).getValue());
         }
      }
   }

   public void testAtomicPutIfAbsentFromNonOwner(Method m) throws Exception {
      String key = k(m), value = "value", value2 = "v2";
      String replaced = getFirstNonOwner(key).putIfAbsent(key, value);
      assertNull(replaced);
      replaced = getFirstNonOwner(key).putIfAbsent(key, value2);
      assertEquals(replaced, value);
      for (Cache<Object, String> c : caches) {
         assertEquals(replaced, c.get(key));
         if (isOwner(c, key)) {
            CacheLoader store = TestingUtil.getFirstLoader(c);
            assertTrue(store.contains(key));
            assertEquals(value, store.load(key).getValue());
         }
      }
   }
   
   public void testAtomicPutIfAbsentFromNonOwnerWithFlag(Method m) throws Exception {
      String key = k(m), value = "value";
      String replaced = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).putIfAbsent(key, value);
      assertNull(replaced);
      //interesting case: fails to put as value exists, put actually missing in Store
      replaced = getFirstNonOwner(key).putIfAbsent(key, value);
      assertEquals(replaced, value);
      for (Cache<Object, String> c : caches) {
         assertEquals(replaced, c.get(key));
         CacheLoader store = TestingUtil.getFirstLoader(c);
         assertFalse(store.contains(key));
      }
   }

   public void testClear() throws Exception {
      prepareClearTest();
      c1.clear();
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      for (int i = 0; i < 5; i++) {
         String key = "k" + i;
         for (Cache<Object, String> c : caches) {
            CacheLoader store = TestingUtil.getFirstLoader(c);
            assertFalse(store.contains(key));
         }
      }
   }

   public void testClearWithFlag() throws Exception {
      prepareClearTest();
      c1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();
      for (Cache<Object, String> c : caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         assertEquals("Data container " + c + " should be empty, instead it contains keys " +
                            dc.keySet(), 0, dc.size());
         CacheLoader store = TestingUtil.getFirstLoader(c);
         for (int i = 0; i < 5; i++) {
            String key = "k" + i;
            if (isOwner(c, key)) {
               assertTrue(store.contains(key));
            }
         }
      }
   }

   public void testGetOnlyQueriesCacheOnOwners() throws PersistenceException {
      // Make a key that own'ers is c1 and c2
      final MagicKey k = new MagicKey("key1", c1, c2);
      final String v1 = "real-data";
      final String v2 = "stale-data";

      // Simulate c3 was by itself and someone wrote a value that is now stale
      CacheWriter store = (CacheWriter) TestingUtil.getFirstLoader(c3);
      store.write(new MarshalledEntryImpl(k, v2, null, marshaller(c3)));

      c1.put(k, v1);

      assertEquals(v1, c3.get(k));
   }
   
   /*---    test helpers      ---*/

   private Map<String, String> makePutAllTestData() {
      Map<String, String> data = new HashMap<String, String>();
      data.put(k1, v1);
      data.put(k2, v2);
      data.put(k3, v3);
      data.put(k4, v4);
      return data;
   }
   
   private void prepareClearTest() throws PersistenceException {
      for (Cache<Object, String> c : caches) assert c.isEmpty() : "Data container " + c + " should be empty, instead it contains keys " + c.keySet();
      for (int i = 0; i < 5; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
      }
      // this will fill up L1 as well
      for (int i = 0; i < 5; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);
      for (Cache<Object, String> c : caches) {
         assertFalse(c.isEmpty());
         CacheLoader store = TestingUtil.getFirstLoader(c);
         for (int i = 0; i < 5; i++) {
            String key = "k" + i;
            if (isOwner(c, key)) {
               assertTrue("Cache store " + c + " does not contain key " + key, store.contains(key));
            }
         }
      }
   }
   
}
