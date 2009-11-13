/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Arrays;
import java.util.Collections;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is a base functional test class containing tests that should be executed for each cache store/loader 
 * implementation. As these are functional tests, they should interact against Cache/CacheManager only and
 * any access to the underlying cache store/loader should be done to verify contents. 
 */
@Test(groups = "unit", testName = "loaders.BaseCacheStoreFunctionalTest")
public abstract class BaseCacheStoreFunctionalTest extends AbstractInfinispanTest {
   
   protected abstract CacheStoreConfig createCacheStoreConfig() throws Exception;
   
   protected CacheStoreConfig csConfig;
   
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
      CacheManager localCacheManager = TestCacheManagerFactory.createLocalCacheManager();
      try {
         GlobalConfiguration configuration = localCacheManager.getGlobalConfiguration();
         CacheLoaderManagerConfig clmConfig = new CacheLoaderManagerConfig();
         clmConfig.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig)csConfig));
         configuration.getDefaultConfiguration().setCacheLoaderManagerConfig(clmConfig);
         localCacheManager.defineConfiguration("first", new Configuration());
         localCacheManager.defineConfiguration("second", new Configuration());

         Cache first = localCacheManager.getCache("first");
         Cache second = localCacheManager.getCache("second");
         assert first.getConfiguration().getCacheLoaderManagerConfig().getCacheLoaderConfigs().size() == 1;
         assert second.getConfiguration().getCacheLoaderManagerConfig().getCacheLoaderConfigs().size() == 1;

         first.start();
         second.start();

         first.put("key", "val");
         assert first.get("key").equals("val");
         assert second.get("key") == null;

         second.put("key2","val2");
         assert second.get("key2").equals("val2");
         assert first.get("key2") == null;         
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }
   
   public void testPreloading() {
      doRunPreloadingTest(60000);
      doRunPreloadingTest(60000);
      doRunPreloadingTest(70000);
      doRunPreloadingTest(70000);
   }
   
   private void doRunPreloadingTest(int count) {
      CacheManager local = TestCacheManagerFactory.createLocalCacheManager();
      try {
         CacheLoaderManagerConfig cacheLoaders = new CacheLoaderManagerConfig();
         cacheLoaders.setPreload(true);
         CacheLoaderManagerConfig clmConfig = new CacheLoaderManagerConfig();
         clmConfig.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig)csConfig));
         local.getGlobalConfiguration().getDefaultConfiguration().setCacheLoaderManagerConfig(clmConfig);
         Cache cache = local.getCache("testPreloading");
         cache.start();
         byte[] bytes = new byte[count];
         Arrays.fill(bytes, (byte) 1);
         cache.put("test_object", bytes);
         int cacheSize =  cache.size();
         assert 1 == cacheSize;      
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }
}
