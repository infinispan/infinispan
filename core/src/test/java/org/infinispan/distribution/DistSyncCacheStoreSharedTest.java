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
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * DistSyncCacheStoreSharedTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.DistSyncCacheStoreSharedTest")
public class DistSyncCacheStoreSharedTest extends BaseDistCacheStoreTest {

   public DistSyncCacheStoreSharedTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      shared = true;
   }

   @AfterMethod
   public void clearStats() {
      for (Cache<?, ?> c: caches) {
         log.trace("Clearing stats for cache store on cache "+ c);
         DummyInMemoryCacheStore cs = (DummyInMemoryCacheStore) TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         cs.clearStats();
      }
   }

   public void testPutFromNonOwner() throws Exception {
      String key = "k4", value = "value4";
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      Cache<Object, String> nonOwner = getFirstNonOwner(key);
      CacheStore nonOwnerStore = TestingUtil.extractComponent(nonOwner, CacheLoaderManager.class).getCacheStore();
      assert !nonOwnerStore.containsKey(key);
      Object retval = nonOwner.put(key, value);
      asyncWait(key, PutKeyValueCommand.class, getSecondNonOwner(key));

      Cache[] owners = getOwners(key);
      CacheStore store = TestingUtil.extractComponent(owners[0], CacheLoaderManager.class).getCacheStore();
      assertIsInContainerImmortal(owners[0], key);
      assert store.containsKey(key);

      for (int i = 1; i < owners.length; i++) {
         store = TestingUtil.extractComponent(owners[i], CacheLoaderManager.class).getCacheStore();
         assertIsInContainerImmortal(owners[i], key);
         assert store.containsKey(key);
      }

      for (Cache<Object, String> c : caches) {
         store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         assert store.containsKey(key);
         assertNumberOfInvocations(store, "store", 1);
      }

      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }

   public void testPutFromOwner() throws Exception {
      String key = "k5", value = "value5";
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      Cache[] owners = getOwners(key);
      Object retval = owners[0].put(key, value);
      asyncWait(key, PutKeyValueCommand.class, getNonOwners(key));
      CacheStore store = TestingUtil.extractComponent(owners[0], CacheLoaderManager.class).getCacheStore();
      assertIsInContainerImmortal(owners[0], key);
      assert store.containsKey(key);

      for (int i = 1; i < owners.length; i++) {
         store = TestingUtil.extractComponent(owners[i], CacheLoaderManager.class).getCacheStore();
         assertIsInContainerImmortal(owners[i], key);
         assert store.containsKey(key);
      }

      for (Cache<Object, String> c : caches) {
         store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
         }
         assert store.containsKey(key);
         assertNumberOfInvocations(store, "store", 1);
      }

      if (testRetVals) assert retval == null;
      assertOnAllCachesAndOwnership(key, value);
   }

   private void assertNumberOfInvocations(CacheStore cs, String method, int expected) {
      int actual = ((DummyInMemoryCacheStore) cs).stats().get(method);
      assert expected == actual : "Expected " + expected + " but was " + actual;
   }


   public void testPutAll() throws Exception {
      log.trace("Here it begins");
      String k1 = "1", v1 = "one", k2 = "2", v2 = "two", k3 = "3", v3 = "three", k4 = "4", v4 = "four";
      String[] keys = new String[]{k1, k2, k3, k4};
      Map<String, String> data = new HashMap<String, String>();
      data.put(k1, v1);
      data.put(k2, v2);
      data.put(k3, v3);
      data.put(k4, v4);

      c1.putAll(data);
      for (String key : keys) {
         for (Cache<Object, String> c : caches) {
            CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
            if (isFirstOwner(c, key)) {
               assertIsInContainerImmortal(c, key);
            }
            log.debug("Testing " + c);
            assertNumberOfInvocations(store, "store", 4);
            assert store.containsKey(key);
         }
      }
   }

   public void testRemoveFromNonOwner() throws Exception {
      String key = "k1", value = "value";
      initAndTest();

      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isFirstOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert store.load(key).getValue().equals(value);
         }
      }

      Object retval = getFirstNonOwner(key).remove(key);
      asyncWait("k1", RemoveCommand.class, getSecondNonOwner("k1"));
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         assert !store.containsKey(key);
         assertNumberOfInvocations(store, "remove", 1);
      }
   }

   public void testReplaceFromNonOwner() throws Exception {
      String key = "k1", value = "value", value2 = "v2";
      initAndTest();

      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isFirstOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            assert store.load(key).getValue().equals(value);
         }
      }

      Object retval = getFirstNonOwner(key).replace(key, value2);
      asyncWait(key, ReplaceCommand.class, getSecondNonOwner(key));
      if (testRetVals) assert value.equals(retval);
      for (Cache<Object, String> c : caches) {
         CacheStore store = TestingUtil.extractComponent(c, CacheLoaderManager.class).getCacheStore();
         if (isFirstOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
         }
         assert store.load(key).getValue().equals(value2);
         assertNumberOfInvocations(store, "store", 2);
      }
   }

   public void testClear() throws Exception {
      for (Cache<Object, String> c : caches) assert c.isEmpty();
      for (int i = 0; i < 5; i++) {
         getOwners("k" + i)[0].put("k" + i, "value" + i);
         asyncWait("k" + i, PutKeyValueCommand.class, getNonOwners("k" + i));
      }
      // this will fill up L1 as well
      for (int i = 0; i < 5; i++) assertOnAllCachesAndOwnership("k" + i, "value" + i);
      for (Cache<Object, String> c : caches) assert !c.isEmpty();
      c1.clear();
      asyncWait(null, ClearCommand.class);
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      /* We only check c1 because on a shared situation, no matter where the clear is called,
       * it should clear the whole store regardless. Bear in mind that in the test, even though
       * the cache store is shared, each cache has each own cache store, that allows for checking
       * who execute puts, removes...etc. */
      CacheStore store = TestingUtil.extractComponent(c1, CacheLoaderManager.class).getCacheStore();
      for (int i = 0; i < 5; i++) {
         String key = "k" + i;
         assert !store.containsKey(key);
         assertNumberOfInvocations(store, "clear", 1);
      }
   }
}
