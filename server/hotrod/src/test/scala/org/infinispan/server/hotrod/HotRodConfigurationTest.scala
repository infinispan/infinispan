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
import org.infinispan.config.Configuration
import org.infinispan.loaders.cluster.{ClusterCacheLoaderConfig, ClusterCacheLoader}

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
      val props = new Properties
      props.setProperty(PROP_KEY_TOPOLOGY_LOCK_TIMEOUT, "26000")
      props.setProperty(PROP_KEY_TOPOLOGY_REPL_TIMEOUT, "31000")
      withClusteredServer(props) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.getLockAcquisitionTimeout, 26000)
         assertEquals(cfg.getSyncReplTimeout, 31000)
         assertTrue(cfg.isStateTransferEnabled)
         assertEquals(cfg.getStateRetrievalTimeout, 31000 + distSyncTimeout)
         assertNull(cfg.getCacheLoaderManagerConfig.getFirstCacheLoaderConfig)
      }
   }

   def testLazyLoadTopology {
      val props = new Properties
      props.setProperty(PROP_KEY_TOPOLOGY_STATE_TRANSFER, "false")
      props.setProperty(PROP_KEY_TOPOLOGY_REPL_TIMEOUT, "43000")
      withClusteredServer(props) { (cfg, distSyncTimeout) =>
         assertEquals(cfg.getSyncReplTimeout, 43000)
         assertTrue(cfg.isStateTransferEnabled)
         val clcfg = cfg.getCacheLoaders.get(0)
         assertNotNull(clcfg)
         assertEquals(clcfg.getCacheLoaderClassName, classOf[ClusterCacheLoader].getName)
         assertEquals(clcfg.asInstanceOf[ClusterCacheLoaderConfig].getRemoteCallTimeout, 43000)
      }
   }

   private def withClusteredServer(props: Properties) (assert: (Configuration, Long) => Unit) {
      val cacheManager = TestCacheManagerFactory.createClusteredCacheManager
      val server = startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, props)
      try {
         val cfg = cacheManager.getCache(ADDRESS_CACHE_NAME).getConfiguration
         assert(cfg, cacheManager.getGlobalConfiguration.getDistributedSyncTimeout)
      } finally {
         server.stop
         cacheManager.stop
      }
   }
}