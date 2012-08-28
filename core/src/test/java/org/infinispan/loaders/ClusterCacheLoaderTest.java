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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.loaders.cluster.ClusterCacheLoader}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.ClusterCacheLoaderTest")
public class ClusterCacheLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder config1 = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      config1.loaders().addClusterCacheLoader();

      ConfigurationBuilder config2 = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      config2.loaders().addClusterCacheLoader();
      config2.loaders().addStore().cacheStore(new DummyInMemoryCacheStore());

      cacheManager1.defineConfiguration("clusteredCl", config1.build());
      cacheManager2.defineConfiguration("clusteredCl", config2.build());
      waitForClusterToForm("clusteredCl");
   }

   public void testRemoteLoad() {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      assert cache1.get("key") == null;
      assert cache1.get("key") == null;
      cache2.put("key", "value");
      assert "value".equals(cache1.get("key"));
   }

   public void testRemoteLoadFromCacheLoader() throws Exception {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      CacheLoaderManager manager2 = cache2.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      ChainingCacheStore chainingCacheStore = (ChainingCacheStore) manager2.getCacheStore();
      CacheStore cs2 = chainingCacheStore.getStores().keySet().iterator().next();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
      cs2.store(TestInternalCacheEntryFactory.create("key", "value"));
      assert cs2.load("key").getValue().equals("value");
      assert cache1.get("key").equals("value");
   }
}
