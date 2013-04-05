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

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.LocalDeltaAwarePassivationTest")
@CleanupAfterMethod
public class LocalDeltaAwarePassivationTest extends LocalDeltaAwareEvictionTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      configBuilder.eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .loaders().passivation(true).addStore().cacheStore(new DummyInMemoryCacheStore());

      addClusterEnabledCacheManager(configBuilder);
   }

   protected void assertNumberOfEntries(int cacheIndex) throws Exception {
      CacheStore cacheStore = TestingUtil.extractComponent(cache(cacheIndex), CacheLoaderManager.class).getCacheStore();
      assertEquals(1, cacheStore.loadAllKeys(null).size()); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
