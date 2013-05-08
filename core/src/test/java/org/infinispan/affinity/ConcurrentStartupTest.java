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

package org.infinispan.affinity;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "affinity.ConcurrentStartupTest")
public class ConcurrentStartupTest extends AbstractCacheTest {

   public static final int KEY_QUEUE_SIZE = 100;
   EmbeddedCacheManager manager1 = null, manager2 = null;
   AdvancedCache<Object, Object> cache1 = null, cache2 = null;
   KeyAffinityService<Object> keyAffinityService1 = null, keyAffinityService2 = null;
   private ExecutorService ex1;
   private ExecutorService ex2;

   @BeforeMethod
   protected void setUp() throws Exception {
      ConfigurationBuilder configurationBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      configurationBuilder.transaction().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().hash().numOwners(1)
            .clustering().stateTransfer().fetchInMemoryState(false);

      manager1 = TestCacheManagerFactory.createClusteredCacheManager(configurationBuilder);
      manager1.defineConfiguration("test", configurationBuilder.build());
      cache1 = manager1.getCache("test").getAdvancedCache();
      ex1 = Executors.newSingleThreadExecutor();
      keyAffinityService1 = KeyAffinityServiceFactory.newLocalKeyAffinityService(cache1, new RndKeyGenerator(), ex1, KEY_QUEUE_SIZE);
      log.trace("Address for manager1: " + manager1.getAddress());

      manager2 = TestCacheManagerFactory.createClusteredCacheManager(configurationBuilder);
      manager2.defineConfiguration("test", configurationBuilder.build());
      cache2 = manager2.getCache("test").getAdvancedCache();
      ex2 = Executors.newSingleThreadExecutor();
      keyAffinityService2 = KeyAffinityServiceFactory.newLocalKeyAffinityService(cache2, new RndKeyGenerator(), ex2, KEY_QUEUE_SIZE);
      log.trace("Address for manager2: " + manager2.getAddress());

      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
      Thread.sleep(5000);
   }

   @AfterTest
   protected void tearDown() throws Exception {
      if (ex1 != null)
         ex1.shutdownNow();
      if (ex2 != null)
         ex2.shutdownNow();
      if (keyAffinityService1 != null)  keyAffinityService1.stop();
      if (keyAffinityService2 != null) keyAffinityService2.stop();
      TestingUtil.killCacheManagers(manager1, manager2);
   }

   public void testKeyAffinityServiceFails() {
      log.trace("Test keys for cache2.");
      for (int i = 0; i < KEY_QUEUE_SIZE; i++) {
         Object keyForAddress = keyAffinityService2.getKeyForAddress(manager2.getAddress());
         assertTrue(cache1.getDistributionManager().locate(keyForAddress).contains(manager2.getAddress()));
      }
      log.trace("Test keys for cache1.");
      for (int i = 0; i < KEY_QUEUE_SIZE; i++) {
         Object keyForAddress = keyAffinityService1.getKeyForAddress(manager1.getAddress());
         List<Address> locate = cache1.getDistributionManager().locate(keyForAddress);
         assertTrue("For key " + keyForAddress + " Locate " + locate + " should contain " + manager1.getAddress(), locate.contains(manager1.getAddress()));
      }
   }
}
