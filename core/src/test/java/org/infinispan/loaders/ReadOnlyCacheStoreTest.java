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

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

/**
 * Test read only store,
 * i.e. test proper functionality of setting ignoreModifications(true) for cache store.
 *
 * @author Tomas Sykora
 */
@Test(testName = "loaders.ReadOnlyCacheStoreTest", groups = "functional", sequential = true)
@CleanupAfterMethod
public class ReadOnlyCacheStoreTest extends SingleCacheManagerTest {
   CacheStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = getDefaultStandaloneConfig(true);
      cfg.setInvocationBatchingEnabled(true);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg().ignoreModifications(true));
      cfg.setCacheLoaderManagerConfig(clmc);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
   }

   public void testReadOnlyCacheStore() throws CacheLoaderException {
      // ignore modifications
      store.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store.store(TestInternalCacheEntryFactory.create("k2", "v2"));

      assert !store.containsKey("k1") : "READ ONLY - Store should NOT contain k1 key.";
      assert !store.containsKey("k2") : "READ ONLY - Store should NOT contain k2 key.";

      // put into cache but not into read only store
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));

      assert !store.containsKey("k1") : "READ ONLY - Store should NOT contain k1 key.";
      assert !store.containsKey("k2") : "READ ONLY - Store should NOT contain k2 key.";

      assert !store.remove("k1") : "READ ONLY - Remove operation should return false (no op)";
      assert !store.remove("k2") : "READ ONLY - Remove operation should return false (no op)";
      assert !store.remove("k3") : "READ ONLY - Remove operation should return false (no op)";

      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));
      cache.remove("k1");
      cache.remove("k2");
      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }
}
