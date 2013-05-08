/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(testName = "loaders.PassivatePersistentTest", groups = "functional")
public class PassivatePersistentTest extends AbstractInfinispanTest {

   Cache<String, String> cache;
   CacheStore store;
   TransactionManager tm;
   Configuration cfg;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cfg = new Configuration().fluent()
         .loaders()
            .passivation(true)
            .addCacheLoader(new DummyInMemoryCacheStore.Cfg()
               .storeName(this.getClass().getName())
               .purgeOnStartup(false))
         .build();
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      store.clear();
      TestingUtil.killCacheManagers(cm);
   }

   public void testPersistence() throws CacheLoaderException {
      cache.put("k", "v");
      assert "v".equals(cache.get("k"));
      cache.evict("k");
      assert store.containsKey("k");

      assert "v".equals(cache.get("k"));
      assert !store.containsKey("k");

      cache.stop();
      cache.start();
      // The old store's marshaller is not working any more
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();

      assert store.containsKey("k");
      assert "v".equals(cache.get("k"));
      assert !store.containsKey("k");
   }
}
