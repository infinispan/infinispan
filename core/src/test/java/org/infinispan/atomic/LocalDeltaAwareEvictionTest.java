/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.LocalDeltaAwareEvictionTest")
@CleanupAfterMethod
public class LocalDeltaAwareEvictionTest extends MultipleCacheManagersTest {

   protected static final String KEY1 = "key1";

   protected static final String KEY2 = "key2";

   protected boolean txEnabled = false;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      configBuilder.eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .loaders().addStore().cacheStore(new DummyInMemoryCacheStore());

      addClusterEnabledCacheManager(configBuilder);
   }

   /**
    * A generic accessor for a DeltaAware that consists of two String components.
    * Implementations of this interface will plug into the test and will drive access to the actual DeltaAware
    * instances allowing the test logic to be reused for different kinds of DeltaAware.
    *
    * @param <TDA> the type of DeltaAware
    * @param <TK>  the type of cache key
    */
   interface DeltaAwareAccessor<TDA, TK> {

      TDA createObject(Cache cache, TK key);

      TDA getObject(Cache cache, TK key);

      void putObject(Cache cache, TK key, TDA da);

      String getFirstComponent(TDA da);

      void setFirstComponent(TDA da, String value);

      String getSecondComponent(TDA da);

      void setSecondComponent(TDA da, String value);
   }

   protected Object withTx(int cacheIndex, Callable<Object> c) throws Exception {
      if (txEnabled) {
         return TestingUtil.withTx(cache(cacheIndex).getAdvancedCache().getTransactionManager(), c);
      } else {
         return c.call();
      }
   }

   protected void assertNumberOfEntries(int cacheIndex) throws Exception {
      CacheStore cacheStore = TestingUtil.extractComponent(cache(cacheIndex), CacheLoaderManager.class).getCacheStore();
      assertEquals(2, cacheStore.loadAllKeys(null).size()); // two entries in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }

   protected void test(final DeltaAwareAccessor daa, final int nodeThatReads, final int nodeThatWrites) throws Exception {
      // create two delta-aware objects populated with initial values for components
      withTx(nodeThatWrites, new Callable<Object>() {
         public Object call() {
            daa.createObject(cache(nodeThatWrites), KEY1);
            daa.createObject(cache(nodeThatWrites), KEY2);
            return null;
         }
      });

      // check initial values not lost due to eviction
      assertInitialValues(daa, nodeThatWrites);
      if (nodeThatReads != nodeThatWrites) {
         assertInitialValues(daa, nodeThatReads);
      }

      // update first component of both objects
      withTx(nodeThatWrites, new Callable<Object>() {
         public Object call() throws Exception {
            Object obj1 = daa.getObject(cache(nodeThatWrites), KEY1);
            daa.setFirstComponent(obj1, "** UPDATED** first component of object with key=key1");
            daa.putObject(cache(nodeThatWrites), KEY1, obj1);

            Object obj2 = daa.getObject(cache(nodeThatWrites), KEY2);
            daa.setFirstComponent(obj2, "** UPDATED** first component of object with key=key2");
            daa.putObject(cache(nodeThatWrites), KEY2, obj2);
            return null;
         }
      });

      assertUpdatedValues(daa, nodeThatWrites);
      if (nodeThatReads != nodeThatWrites) {
         assertUpdatedValues(daa, nodeThatReads);
      }
   }

   protected void assertInitialValues(DeltaAwareAccessor daa, int cacheIndex) throws Exception {
      assertNumberOfEntries(cacheIndex);

      final Object obj1 = daa.getObject(cache(cacheIndex), KEY1);

      assertNotNull(obj1);
      assertEquals("first component of object with key=" + KEY1, daa.getFirstComponent(obj1));
      assertEquals("second component of object with key=" + KEY1, daa.getSecondComponent(obj1));

      final Object obj2 = daa.getObject(cache(cacheIndex), KEY2);

      assertNotNull(obj2);
      assertEquals("first component of object with key=" + KEY2, daa.getFirstComponent(obj2));
      assertEquals("second component of object with key=" + KEY2, daa.getSecondComponent(obj2));

      assertNumberOfEntries(cacheIndex);
   }

   protected void assertUpdatedValues(DeltaAwareAccessor daa, int nodeThatReads) throws Exception {
      assertNumberOfEntries(nodeThatReads);

      Object obj1 = daa.getObject(cache(nodeThatReads), KEY1);
      assertNotNull(obj1);
      assertEquals("** UPDATED** first component of object with key=" + KEY1, daa.getFirstComponent(obj1));
      assertEquals("second component of object with key=" + KEY1, daa.getSecondComponent(obj1));

      Object obj2 = daa.getObject(cache(nodeThatReads), KEY2);
      assertNotNull(obj2);
      assertEquals("** UPDATED** first component of object with key=" + KEY2, daa.getFirstComponent(obj2));
      assertEquals("second component of object with key=" + KEY2, daa.getSecondComponent(obj2));

      assertNumberOfEntries(nodeThatReads);
   }

   public void testDeltaAware() throws Exception {
      test(createDeltaAwareAccessor(), 0, 0);
   }

   protected DeltaAwareAccessor<TestDeltaAware, String> createDeltaAwareAccessor() {
      return new DeltaAwareAccessor<TestDeltaAware, String>() {

         @Override
         public TestDeltaAware createObject(Cache cache, String key) {
            TestDeltaAware da = new TestDeltaAware();
            da.setFirstComponent("first component of object with key=" + key);
            da.setSecondComponent("second component of object with key=" + key);
            cache.put(key, da);
            return da;
         }

         @Override
         public TestDeltaAware getObject(Cache cache, String key) {
            return (TestDeltaAware) cache.get(key);
         }

         @Override
         public void putObject(Cache cache, String key, TestDeltaAware da) {
            cache.put(key, da);
         }

         @Override
         public String getFirstComponent(TestDeltaAware da) {
            return da.getFirstComponent();
         }

         @Override
         public void setFirstComponent(TestDeltaAware da, String value) {
            da.setFirstComponent(value);
         }

         @Override
         public String getSecondComponent(TestDeltaAware da) {
            return da.getSecondComponent();
         }

         @Override
         public void setSecondComponent(TestDeltaAware da, String value) {
            da.setSecondComponent(value);
         }
      };
   }

   public void testAtomicMap() throws Exception {
      test(createAtomicMapAccessor(), 0, 0);
   }

   protected DeltaAwareAccessor<Map<String, String>, String> createAtomicMapAccessor() {
      return new DeltaAwareAccessor<Map<String, String>, String>() {

         @Override
         public Map<String, String> createObject(Cache cache, String key) {
            Map<String, String> map = AtomicMapLookup.getAtomicMap(cache, key);
            map.put("first", "first component of object with key=" + key);
            map.put("second", "second component of object with key=" + key);
            return map;
         }

         @Override
         public Map<String, String> getObject(Cache cache, String key) {
            return AtomicMapLookup.getAtomicMap(cache, key, false);
         }

         @Override
         public void putObject(Cache cache, String key, Map<String, String> da) {
            assertTrue(da instanceof AtomicHashMapProxy);
            // we do not actually put the map back into cache because it must already be there
         }

         @Override
         public String getFirstComponent(Map<String, String> da) {
            return da.get("first");
         }

         @Override
         public void setFirstComponent(Map<String, String> da, String value) {
            da.put("first", value);
         }

         @Override
         public String getSecondComponent(Map<String, String> da) {
            return da.get("second");
         }

         @Override
         public void setSecondComponent(Map<String, String> da, String value) {
            da.put("second", value);
         }
      };
   }

   public void testFineGrainedAtomicMap() throws Exception {
      test(createFineGrainedAtomicMapAccessor(), 0, 0);
   }

   protected DeltaAwareAccessor<Map<String, String>, String> createFineGrainedAtomicMapAccessor() {
      return new DeltaAwareAccessor<Map<String, String>, String>() {

         @Override
         public Map<String, String> createObject(Cache cache, String key) {
            Map<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache, key);
            map.put("first", "first component of object with key=" + key);
            map.put("second", "second component of object with key=" + key);
            return map;
         }

         @Override
         public Map<String, String> getObject(Cache cache, String key) {
            return AtomicMapLookup.getFineGrainedAtomicMap(cache, key, false);
         }

         @Override
         public void putObject(Cache cache, String key, Map<String, String> da) {
            assertTrue(da instanceof FineGrainedAtomicHashMapProxy);
            // we do not actually put the map back into cache because it must already be there
         }

         @Override
         public String getFirstComponent(Map<String, String> da) {
            return da.get("first");
         }

         @Override
         public void setFirstComponent(Map<String, String> da, String value) {
            da.put("first", value);
         }

         @Override
         public String getSecondComponent(Map<String, String> da) {
            return da.get("second");
         }

         @Override
         public void setSecondComponent(Map<String, String> da, String value) {
            da.put("second", value);
         }
      };
   }
}
