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
package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * StateTransferFetchOnlyPersistentStateTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferFetchOnlyPersistentStateTest")
public class StateTransferFetchOnlyPersistentStateTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cfg = createConfiguration(1);
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration("onlyFetchPersistent", cfg.build());
   }

   private ConfigurationBuilder createConfiguration(int id) {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg.clustering().stateTransfer().fetchInMemoryState(false);

      DummyInMemoryCacheStoreConfigurationBuilder dimcs = new DummyInMemoryCacheStoreConfigurationBuilder(cfg.loaders());
      dimcs.storeName("store id: " + id);
      dimcs.fetchPersistentState(true);
      cfg.loaders().shared(false).addLoader(dimcs);

      return cfg;
   }

   public void test000(Method m) {
      final String theKey = "k-" + m.getName();
      final String theValue = "v-" + m.getName();

      Cache cache1 = cache(0, "onlyFetchPersistent");
      assert !cache1.getCacheConfiguration().clustering().stateTransfer().fetchInMemoryState();
      cache1.put(theKey, theValue);

      ConfigurationBuilder cfg2 = createConfiguration(2);
      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("onlyFetchPersistent", cfg2.build());

      Cache cache2 = cache(1, "onlyFetchPersistent");
      assert !cache2.getCacheConfiguration().clustering().stateTransfer().fetchInMemoryState();
      assert cache2.containsKey(theKey);
      assert cache2.get(theKey).equals(theValue);
   }

}
