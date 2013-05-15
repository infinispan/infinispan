/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.k;
import static org.junit.Assert.*;

/**
 * DistSyncSharedTest.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.DistSyncCacheStoreNotSharedTest")
@CleanupAfterMethod
public class DistSyncCacheStoreNotSharedTest extends BaseDistCacheStoreTest {
   
   private static final String k1 = "1", v1 = "one", k2 = "2", v2 = "two", k3 = "3", v3 = "three", k4 = "4", v4 = "four";
   private static final String[] keys = new String[]{k1, k2, k3, k4};

   public DistSyncCacheStoreNotSharedTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      shared = false;
   }

   public void testPutFromNonOwner(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheStore nonOwnerStore = TestingUtil.extractComponent(nonOwner, CacheLoaderManager.class).getCacheStore();
      CacheStore ownerStore = TestingUtil.extractComponent(owner, CacheLoaderManager.class).getCacheStore();
      assert !nonOwnerStore.containsKey(key);
      assert !ownerStore.containsKey(key);
      Object retval = nonOwner.put(key, value);
      assert !nonOwnerStore.containsKey(key);
      assert ownerStore.containsKey(key);
      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }
   
   public void testGetFromNonOwnerWithFlags(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheStore ownerStore = TestingUtil.extractComponent(owner, CacheLoaderManager.class).getCacheStore();
      owner.put(key, value);
      assert value.equals(ownerStore.load(key).getValue());
      owner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();
      assert value.equals(ownerStore.load(key).getValue());
      assert owner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).get(key) == null;
      assert nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).get(key) == null;
      assert value.equals(nonOwner.get(key));
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

      Future<String> async = nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).getAsync(key);
      assert async != null;
      assert async.get() == null;

      async = nonOwner.getAdvancedCache().getAsync(key);
      assert async != null;
      String returnedValue = async.get();
      assert value.equals(returnedValue);
   }

   public void testPutFromNonOwnerWithFlags(Method m) throws Exception {
      String key = k(m), value = "value2";
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      Cache<Object, String> owner = getFirstOwner(key);
      CacheStore nonOwnerStore = TestingUtil.extractComponent(nonOwner, CacheLoaderManager.class).getCacheStore();
      CacheStore ownerStore = TestingUtil.extractComponent(owner, CacheLoaderManager.class).getCacheStore();
      assert !nonOwnerStore.containsKey(key);
      assert !ownerStore.containsKey(key);
      Object retval = nonOwner.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).put(key, value);
      assert !nonOwnerStore.containsKey(key);
      assert !ownerStore.containsKey(key);
      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }

   public void testPutFromOwner(Method m) throws Exception {
      String key = k(m), value = "value3";
      getOwners(key)[0].put(key, value);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert store.containsKey(key);
         } else {
            assertIsNotInL1(c, key);
            assert !store.containsKey(key);
         }
      }
   }

   public void testPutAll() throws Exception {

      c1.putAll(makePutAllTestData());

      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         for (String key : keys) {
            if (isOwner(c, key)) {
               assertIsInContainerImmortal(c, key);
               assert store.containsKey(key);
            } else {
               assert !store.containsKey(key);
            }
         }
      }
   }

   public void testPutAllWithFlags() throws Exception {
      Map<String, String> data = makePutAllTestData();
      c1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).putAll(data);

      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         for (String key : keys) {
            assert !store.containsKey(key);
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
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert store.load(key).getValue().equals(value);
         } else {
            assert !store.containsKey(key);
         }
      }

      Object retval = getFirstNonOwner(key).remove(key);
      if (testRetVals) assert "value".equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         assert !store.containsKey(key);
      }
   }
   
   public void testRemoveFromNonOwnerWithFlags() throws Exception {
      String key = "k1", value = "value";
      initAndTest();
      Object retval = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).remove(key);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         if (isOwner(c, key)) {
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            assert store.containsKey(key);
         }
      }
   }

   public void testReplaceFromNonOwner() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();

      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert store.load(key).getValue().equals(value);
         } else {
            assert !store.containsKey(key);
         }
      }

      Object retval = getFirstNonOwner(key).replace(key, value2);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert c.get(key).equals(value2);
            assert store.load(key).getValue().equals(value2);
         } else {
            assert !store.containsKey(key);
         }
      }
   }
   
   public void testReplaceFromNonOwnerWithFlag() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();
      Object retval = getFirstNonOwner(key).getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).replace(key, value2);
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert c.get(key).equals(value2);
            assert store.load(key).getValue().equals(value);
         } else {
            assert !store.containsKey(key);
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
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            assertTrue(store.containsKey(key));
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
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            assertTrue(store.containsKey(key));
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
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            assertTrue(store.containsKey(key));
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
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         assertFalse(store.containsKey(key));
      }
   }

   public void testClear() throws Exception {
      prepareClearTest();
      c1.clear();
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      for (int i = 0; i < 5; i++) {
         String key = "k" + i;
         for (Cache<Object, String> c : caches) {
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            assert !store.containsKey(key);
         }
      }
   }

   public void testClearWithFlag() throws Exception {
      prepareClearTest();
      c1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();
      for (Cache<Object, String> c : caches) {
         assert c.isEmpty() : "Data container " + c + " should be empty, instead it contains keys " + c.keySet();
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         for (int i = 0; i < 5; i++) {
            String key = "k" + i;
            if (isOwner(c, key)) {
               assert store.containsKey(key);
            }
         }
      }
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
   
   private void prepareClearTest() throws CacheLoaderException {
      for (Cache<Object, String> c : caches) assert c.isEmpty() : "Data container " + c + " should be empty, instead it contains keys " + c.keySet();
      for (int i = 0; i < 5; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
      }
      // this will fill up L1 as well
      for (int i = 0; i < 5; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);
      for (Cache<Object, String> c : caches) {
         assert !c.isEmpty();
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         for (int i = 0; i < 5; i++) {
            String key = "k" + i;
            if (isOwner(c, key)) {
               assert store.containsKey(key) : "Cache store " + c + " does not contain key " + key;
            }
         }
      }
   }
   
}
