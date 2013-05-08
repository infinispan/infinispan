/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.decorators;

import java.io.File;
import java.util.HashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * BatchAsyncCacheStoreTest performs some additional tests on the AsyncStore
 * but using batches.
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "loaders.decorators.BatchAsyncCacheStoreTest")
public class BatchAsyncCacheStoreTest extends SingleCacheManagerTest {

   private final HashMap<Object, Object> cacheCopy = new HashMap<Object, Object>();

   public BatchAsyncCacheStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = new Configuration();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      configuration.setInvocationBatchingEnabled(true);
      enableTestJdbcStorage(configuration);

      return TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getNonClusteredDefault(), configuration);
   }

   private void enableTestJdbcStorage(Configuration configuration) throws Exception {
      CacheStoreConfig fileStoreConfiguration = createCacheStoreConfig();
      AsyncStoreConfig asyncStoreConfig = new AsyncStoreConfig();
      asyncStoreConfig.setEnabled(true);
      asyncStoreConfig.setThreadPoolSize(1);
      fileStoreConfiguration.setAsyncStoreConfig(asyncStoreConfig);
      CacheLoaderManagerConfig loaderManagerConfig = configuration.getCacheLoaderManagerConfig();
      loaderManagerConfig.setPassivation(false);
      loaderManagerConfig.setPreload(false);
      loaderManagerConfig.setShared(true);
      loaderManagerConfig.addCacheLoaderConfig(fileStoreConfiguration);
   }

   @Test
   public void sequantialOvewritingInBatches() {
      cache = cacheManager.getCache();
      AdvancedCache<Object,Object> advancedCache = cache.getAdvancedCache();
      for (int i = 0; i < 2000;) {
         advancedCache.startBatch();
         putAValue(advancedCache, i++);
         putAValue(advancedCache, i++);
         advancedCache.endBatch(true);
      }
      cacheCopy.putAll(cache);
      cache.stop();
      cacheManager.stop();
   }

   private void putAValue(AdvancedCache<Object, Object> advancedCache, int i) {
      String key = "k" + (i % 13);
      String value = "V" + i;
      advancedCache.put(key, value);
   }

   @Test(dependsOnMethods = "sequantialOvewritingInBatches")
   public void indexWasStored() {
      cache = cacheManager.getCache();
      Assert.assertTrue(cache.isEmpty());
      boolean failed = false;
      for (Object key : cacheCopy.keySet()) {
         Object expected = cacheCopy.get(key);
         Object actual = cache.get(key);
         if (!expected.equals(actual)) {
            log.errorf("Failure on key '%s' expected value: '%s' actual value: '%s'", key.toString(), expected, actual);
            failed = true;
         }
      }
      Assert.assertFalse(failed);
      Assert.assertEquals(cacheCopy.keySet().size(), cache.keySet().size(), "have a different number of keys");
   }

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
      new File(tmpDirectory).mkdirs();
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   protected CacheStoreConfig createCacheStoreConfig() {
      FileCacheStoreConfig cfg = new FileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      return cfg;
   }

}
