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
package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is a base functional test class containing tests that should be executed for each cache store/loader
 * implementation. As these are functional tests, they should interact against Cache/CacheManager only and any access to
 * the underlying cache store/loader should be done to verify contents.
 */
@Test(groups = "unit", testName = "loaders.BaseCacheStoreFunctionalTest")
public abstract class BaseCacheStoreFunctionalTest extends AbstractInfinispanTest {

   protected abstract CacheStoreConfig createCacheStoreConfig() throws Exception;

   protected CacheStoreConfig csConfig;
   protected Set<String> cacheNames = new HashSet<String>();

   @BeforeMethod
   public void setUp() throws Exception {
      try {
         csConfig = createCacheStoreConfig();
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
         throw e;
      }
   }

   public void testTwoCachesSameCacheStore() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      try {
         CacheLoaderManagerConfig clmConfig = new CacheLoaderManagerConfig();
         clmConfig.addCacheLoader(csConfig);
         localCacheManager.getDefaultConfiguration().setCacheLoaderManagerConfig(clmConfig);
         localCacheManager.defineConfiguration("first", new Configuration());
         localCacheManager.defineConfiguration("second", new Configuration());
         cacheNames.add("first");
         cacheNames.add("second");

         Cache first = localCacheManager.getCache("first");
         Cache second = localCacheManager.getCache("second");

         first.start();
         second.start();

         first.put("key", "val");
         assert first.get("key").equals("val");
         assert second.get("key") == null;

         second.put("key2", "val2");
         assert second.get("key2").equals("val2");
         assert first.get("key2") == null;
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testPreloadAndExpiry() {
      CacheLoaderManagerConfig cacheLoaders = new CacheLoaderManagerConfig();
      cacheLoaders.setPreload(true);
      cacheLoaders.addCacheLoaderConfig(csConfig);
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(false);
      cfg.setCacheLoaderManagerConfig(cacheLoaders);
      CacheContainer local = TestCacheManagerFactory.createCacheManager(cfg);
      try {
         Cache<String, String> cache = local.getCache();
         cacheNames.add(cache.getName());
         cache.start();

         assert cache.getConfiguration().getCacheLoaderManagerConfig().isPreload();

         cache.put("k1", "v");
         cache.put("k2", "v", 111111, TimeUnit.MILLISECONDS);
         cache.put("k3", "v", -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
         cache.put("k4", "v", 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

         cache.stop();

         cache.start();

         assertCacheEntry(cache, "k1", "v", -1, -1);
         assertCacheEntry(cache, "k2", "v", 111111, -1);
         assertCacheEntry(cache, "k3", "v", -1, 222222);
         assertCacheEntry(cache, "k4", "v", 333333, 444444);
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testPreloadStoredAsBinary() {
      CacheLoaderManagerConfig cacheLoaders = new CacheLoaderManagerConfig();
      cacheLoaders.setPreload(true);
      cacheLoaders.addCacheLoaderConfig(csConfig);
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(false);
      cfg.setUseLazyDeserialization(true);
      cfg.setCacheLoaderManagerConfig(cacheLoaders);
      CacheContainer local = TestCacheManagerFactory.createCacheManager(cfg);
      try {
         Cache<String, Pojo> cache = local.getCache();
         cacheNames.add(cache.getName());
         cache.start();

         assert cache.getConfiguration().getCacheLoaderManagerConfig().isPreload();
         assert cache.getConfiguration().isUseLazyDeserialization();

         cache.put("k1", new Pojo());
         cache.put("k2", new Pojo(), 111111, TimeUnit.MILLISECONDS);
         cache.put("k3", new Pojo(), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
         cache.put("k4", new Pojo(), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

         cache.stop();

         cache.start();

         cache.entrySet();
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public static class Pojo implements Serializable {
   }

   public void testRestoreAtomicMap(Method m) {
      CacheContainer localCacheContainer = getContainerWithCacheLoader();
      try {
         Cache<String, Object> cache = localCacheContainer.getCache();
         cacheNames.add(cache.getName());
         AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
         map.put("a", "b");

         //evict from memory
         cache.evict(m.getName());

         // now re-retrieve the map
         assert AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a").equals("b");
      } finally {
         TestingUtil.killCacheManagers(localCacheContainer);
      }
   }

   public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
      CacheContainer localCacheContainer = getContainerWithCacheLoader();
      try {
         Cache<String, Object> cache = localCacheContainer.getCache();
         cacheNames.add(cache.getName());
         TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
         tm.begin();
         final AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
         map.put("a", "b");
         tm.commit();

         //evict from memory
         cache.evict(m.getName());

         // now re-retrieve the map and make sure we see the diffs
         assert AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a").equals("b");
      } finally {
         TestingUtil.killCacheManagers(localCacheContainer);
      }
   }

   public void testByteArrayKey(Method m) {
      CacheContainer localCacheContainer = getContainerWithCacheLoader();
      try {
         Cache<ByteArrayKey, Object> cache = localCacheContainer.getCache();
         cache.put(new ByteArrayKey(m.getName().getBytes()), "hello");
      } finally {
         TestingUtil.killCacheManagers(localCacheContainer);
      }
   }

   private CacheContainer getContainerWithCacheLoader() {
      Configuration cfg = new Configuration();
      cfg.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      cfg.getCacheLoaderManagerConfig().addCacheLoaderConfig(csConfig);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private void assertCacheEntry(Cache cache, String key, String value, long lifespanMillis, long maxIdleMillis) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "No such entry for key " + key;
      assert Util.safeEquals(ice.getValue(), value) : ice.getValue() + " is not the same as " + value;
      assert ice.getLifespan() == lifespanMillis : "Lifespan " + ice.getLifespan() + " not the same as " + lifespanMillis;
      assert ice.getMaxIdle() == maxIdleMillis : "MaxIdle " + ice.getMaxIdle() + " not the same as " + maxIdleMillis;
      if (lifespanMillis > -1) assert ice.getCreated() > -1 : "Lifespan is set but created time is not";
      if (maxIdleMillis > -1) assert ice.getLastUsed() > -1 : "Max idle is set but last used is not";

   }
}
