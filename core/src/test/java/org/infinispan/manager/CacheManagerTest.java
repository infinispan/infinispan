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
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lifecycle.ComponentStatus;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest extends AbstractInfinispanTest {
   public void testDefaultCache() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);

      try {
         assert cm.getCache().getStatus() == ComponentStatus.RUNNING;
         assert cm.getCache().getName().equals(CacheContainer.DEFAULT_CACHE_NAME);

         try {
            cm.defineConfiguration(CacheContainer.DEFAULT_CACHE_NAME, new Configuration());
            assert false : "Should fail";
         }
         catch (IllegalArgumentException e) {
            assert true; // ok
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUnstartedCachemanager() {
      DefaultCacheManager dcm = new DefaultCacheManager(false);
      assert dcm.getStatus().equals(ComponentStatus.INSTANTIATED);
      assert !dcm.getStatus().allowInvocations();
      Cache<Object, Object> cache = dcm.getCache();
      cache.put("k","v");
      assert cache.get("k").equals("v");
   }

   public void testClashingNames() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Configuration c = new Configuration();
         Configuration firstDef = cm.defineConfiguration("aCache", c);
         Configuration secondDef = cm.defineConfiguration("aCache", c);
         assert firstDef.equals(secondDef);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      CacheContainer cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache c1 = cm.getCache("cache1");
         Cache c2 = cm.getCache("cache2");
         Cache c3 = cm.getCache("cache3");

         assert c1.getStatus() == ComponentStatus.RUNNING;
         assert c2.getStatus() == ComponentStatus.RUNNING;
         assert c3.getStatus() == ComponentStatus.RUNNING;

         cm.stop();

         assert c1.getStatus() == ComponentStatus.TERMINATED;
         assert c2.getStatus() == ComponentStatus.TERMINATED;
         assert c3.getStatus() == ComponentStatus.TERMINATED;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testDefiningConfigurationValidation() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         cm.defineConfiguration("cache1", (Configuration) null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, (Configuration) null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, new Configuration());
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      Configuration c = cm.defineConfiguration("cache1", null, new Configuration());
      assert c.equalsIgnoreName(cm.getDefaultConfiguration()) ;
      
      c = cm.defineConfiguration("cache1", "does-not-exist-cache", new Configuration());
      assert c.equalsIgnoreName(cm.getDefaultConfiguration());
   }

   public void testDefiningConfigurationWithTemplateName() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);

      Configuration c = new Configuration();
      c.setIsolationLevel(IsolationLevel.NONE);
      Configuration oneCacheConfiguration = cm.defineConfiguration("oneCache", c);
      assert oneCacheConfiguration.equalsIgnoreName(c) ;
      assert oneCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      Configuration secondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert oneCacheConfiguration.equalsIgnoreName(secondCacheConfiguration) ;
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setIsolationLevel(IsolationLevel.SERIALIZABLE);
      Configuration anotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert !secondCacheConfiguration.equals(anotherSecondCacheConfiguration);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setExpirationMaxIdle(Long.MAX_VALUE);
      Configuration yetAnotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert yetAnotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert yetAnotherSecondCacheConfiguration.getExpirationMaxIdle() == Long.MAX_VALUE;
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
   }

   public void testDefiningConfigurationOverridingBooleans() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      Configuration c = new Configuration();
      c.setUseLazyDeserialization(true);
      Configuration lazy = cm.defineConfiguration("storeAsBinary", c);
      assert lazy.isUseLazyDeserialization();

      c = new Configuration();
      c.setEvictionStrategy(EvictionStrategy.LRU);
      c.setEvictionMaxEntries(1);
      Configuration lazyLru = cm.defineConfiguration("lazyDeserializationWithLRU", "storeAsBinary", c);
      assert lazyLru.isUseLazyDeserialization();
      assert lazyLru.getEvictionStrategy() == EvictionStrategy.LRU;
   }

   public void testDefineConfigurationTwice() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Configuration override = new Configuration();
         override.fluent().invocationBatching();
         assert override.isInvocationBatchingEnabled();
         assert cm.defineConfiguration("test1", override).isInvocationBatchingEnabled();
         Configuration config = new Configuration();
         config.applyOverrides(override);
         assert config.isInvocationBatchingEnabled();
         assert cm.defineConfiguration("test2", config).isInvocationBatchingEnabled();
      } finally {
         cm.stop();
      }
   }

   public void testGetCacheNames() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         cm.defineConfiguration("one", new Configuration());
         cm.defineConfiguration("two", new Configuration());
         cm.getCache("three");
         Set<String> cacheNames = cm.getCacheNames();
         assert 3 == cacheNames.size();
         assert cacheNames.contains("one");
         assert cacheNames.contains("two");
         assert cacheNames.contains("three");
      } finally {
         cm.stop();
      }
   }

   public void testCacheStopTwice() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         cache.stop();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testCacheManagerStopTwice() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         localCacheManager.stop();
         localCacheManager.stop();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopManagerStopFollowedByGetCache() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         localCacheManager.getCache();
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCacheStopManagerStopFollowedByCacheOp() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         Cache cache = localCacheManager.getCache();
         cache.put("k", "v");
         cache.stop();
         localCacheManager.stop();
         cache.put("k", "v2");
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testRemoveNonExistentCache(Method m) {
      EmbeddedCacheManager manager = getManagerWithStore(m, false, false);
      try {
         manager.getCache("cache");
         // An attempt to remove a non-existing cache should be a no-op
         manager.removeCache("does-not-exist");
      } finally {
         manager.stop();
      }
   }

   public void testRemoveCacheLocal(Method m) {
      EmbeddedCacheManager manager = getManagerWithStore(m, false, false);
      try {
         Cache cache = manager.getCache("cache");
         cache.put(k(m, 1), v(m, 1));
         cache.put(k(m, 2), v(m, 2));
         cache.put(k(m, 3), v(m, 3));
         DummyInMemoryCacheStore store = getDummyStore(cache);
         DataContainer data = getDataContainer(cache);
         assert !store.isEmpty();
         assert 0 != data.size();
         manager.removeCache("cache");
         assert store.isEmpty();
         assert 0 == data.size();
         // Try removing the cache again, it should be a no-op
         manager.removeCache("cache");
         assert store.isEmpty();
         assert 0 == data.size();
      } finally {
         manager.stop();
      }
   }

   public void testRemoveCacheClusteredLocalStores(Method m) {
      doTestRemoveCacheClustered(m, false);
   }

   public void testRemoveCacheClusteredSharedStores(Method m) {
      doTestRemoveCacheClustered(m, true);
   }

   private EmbeddedCacheManager getManagerWithStore(Method m, boolean isClustered, boolean isStoreShared) {
      return getManagerWithStore(m, isClustered, isStoreShared, "store-");
   }

   private EmbeddedCacheManager getManagerWithStore(Method m, boolean isClustered, boolean isStoreShared, String storePrefix) {
      String storeName = storePrefix + m.getName();
      Configuration c = new Configuration();
      FluentConfiguration.LoadersConfig loaders = c.fluent().loaders();
      if (isStoreShared)
         loaders = loaders.shared(true);

      loaders.addCacheLoader(
            new DummyInMemoryCacheStore.Cfg(storeName));
      if (isClustered) {
         c.fluent().clustering().mode(Configuration.CacheMode.REPL_SYNC);
         return TestCacheManagerFactory.createClusteredCacheManager(c);
      }

      return TestCacheManagerFactory.createCacheManager(c);
   }
   private void doTestRemoveCacheClustered(Method m, boolean isStoreShared) {
      EmbeddedCacheManager manager1 = getManagerWithStore(m, true, isStoreShared, "store1-");
      EmbeddedCacheManager manager2 = getManagerWithStore(m, true, isStoreShared, "store2-");
      try {
         Cache cache1 = manager1.getCache("cache", true);
         Cache cache2 = manager2.getCache("cache", true);
         assert cache1 != null;
         assert cache2 != null;
         assert manager1.cacheExists("cache");
         assert manager2.cacheExists("cache");
         cache1.put(k(m, 1), v(m, 1));
         cache1.put(k(m, 2), v(m, 2));
         cache1.put(k(m, 3), v(m, 3));
         cache2.put(k(m, 4), v(m, 4));
         cache2.put(k(m, 5), v(m, 5));
         DummyInMemoryCacheStore store1 = getDummyStore(cache1);
         DataContainer data1 = getDataContainer(cache1);
         DummyInMemoryCacheStore store2 = getDummyStore(cache2);
         DataContainer data2 = getDataContainer(cache2);
         assert !store1.isEmpty();
         assert 5 == data1.size();
         assert !store2.isEmpty();
         assert 5 == data2.size();
         manager1.removeCache("cache");
         assert !manager1.cacheExists("cache");
         assert !manager2.cacheExists("cache");
         assert null == manager1.getCache("cache", false);
         assert null == manager2.getCache("cache", false);
         assert store1.isEmpty();
         assert 0 == data1.size();
         assert store2.isEmpty();
         assert 0 == data2.size();
      } finally {
         manager1.stop();
         manager2.stop();
      }
   }

   private DummyInMemoryCacheStore getDummyStore(Cache cache1) {
      return (DummyInMemoryCacheStore)
                  TestingUtil.extractComponent(cache1, CacheLoaderManager.class).getCacheLoader();
   }

   private DataContainer getDataContainer(Cache cache) {
      return TestingUtil.extractComponent(cache, DataContainer.class);
   }

}
