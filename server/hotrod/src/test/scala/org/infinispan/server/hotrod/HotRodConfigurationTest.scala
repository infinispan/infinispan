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

package org.infinispan.server.hotrod

import test.UniquePortThreadLocal
import test.HotRodTestingUtil._
import java.util.Properties
import org.infinispan.server.core.Main._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.Assert._
import org.testng.annotations.Test
import org.infinispan.loaders.cluster.{ClusterCacheLoaderConfig, ClusterCacheLoader}
import org.infinispan.server.core.test.Stoppable
import org.infinispan.configuration.cache.{LegacyLoaderConfiguration, Configuration}
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.configuration.cache.ClusterCacheLoaderConfiguration

/**
 * Test to verify that configuration changes are reflected in backend caches.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConfigurationTest")
class HotRodConfigurationTest {

   import HotRodServer.ADDRESS_CACHE_NAME

   def testUserDefinedTimeouts {
      val builder = new HotRodServerConfigurationBuilder
      builder.topologyLockTimeout(26000).topologyReplTimeout(31000)
      withClusteredServer(builder) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.locking().lockAcquisitionTimeout(), 26000)
         assertEquals(cfg.clustering().sync().replTimeout(), 31000)
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState())
         assertEquals(cfg.clustering().stateTransfer().timeout(), 31000 + distSyncTimeout)
         assertTrue(cfg.loaders().cacheLoaders().isEmpty)
      }
   }

   def testLazyLoadTopology {
      val builder = new HotRodServerConfigurationBuilder
      builder.topologyStateTransfer(false).topologyReplTimeout(43000);
      withClusteredServer(builder) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.clustering().sync().replTimeout(), 43000)
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState())
         val clcfg = cfg.loaders().cacheLoaders().get(0).asInstanceOf[ClusterCacheLoaderConfiguration]
         assertNotNull(clcfg)
         assertEquals(clcfg.remoteCallTimeout(), 43000)
      }
   }

   private def withClusteredServer(builder: HotRodServerConfigurationBuilder) (assert: (Configuration, Long) => Unit) {
      Stoppable.useCacheManager(TestCacheManagerFactory.createClusteredCacheManager) { cm =>
         Stoppable.useServer(startHotRodServer(cm, UniquePortThreadLocal.get.intValue, builder)) { server =>
            val cfg = cm.getCache(ADDRESS_CACHE_NAME).getCacheConfiguration
            assert(cfg, cm.getCacheManagerConfiguration.transport().distributedSyncTimeout())
         }
      }
   }
}