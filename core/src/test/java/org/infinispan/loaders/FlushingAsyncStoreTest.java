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
package org.infinispan.loaders;

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * FlushingAsyncStoreTest.
 * 
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "loaders.FlushingAsyncStoreTest", sequential = true)
public class FlushingAsyncStoreTest extends SingleCacheManagerTest {

   /** to assert the test methods are run in proper order **/
   private boolean storeWasRun = false;

   public FlushingAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration config = getDefaultStandaloneConfig(false).fluent()
         .loaders()
            .addCacheLoader(new SlowCacheStoreConfig()
               .storeName(this.getClass().getName())
               .asyncStore().threadPoolSize(1)
               .build())
         .build();
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test(timeOut = 10000)
   public void writeOnStorage() {
      cache = cacheManager.getCache("AsyncStoreInMemory");
      cache.put("key1", "value");
      cache.stop();
      storeWasRun = true;
   }

   @Test(dependsOnMethods = "writeOnStorage")
   public void verifyStorageContent() {
      assert storeWasRun;
      cache = cacheManager.getCache("AsyncStoreInMemory");
      assert "value".equals(cache.get("key1"));
   }
   
   public static class SlowCacheStoreConfig extends DummyInMemoryCacheStore.Cfg {
      public SlowCacheStoreConfig() {
         setCacheLoaderClassName(SlowCacheStore.class.getName());
      }
   }

   public static class SlowCacheStore extends DummyInMemoryCacheStore {
      private void insertDelay() {
         TestingUtil.sleepThread(100);
      }

      @Override
      public void store(InternalCacheEntry ed) {
         insertDelay();
         super.store(ed);
      }
   }
}
