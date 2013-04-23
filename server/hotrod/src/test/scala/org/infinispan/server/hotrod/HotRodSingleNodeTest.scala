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
package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.server.core.CacheValue
import test.HotRodClient
import org.infinispan.AdvancedCache
import test.HotRodTestingUtil._
import org.jboss.netty.channel.ChannelFuture
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.util.ByteArrayKey
import org.infinispan.server.core.test.ServerTestingUtil._
import org.testng.annotations.{Test, AfterClass}

/**
 * Base test class for single node Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   val cacheName = "HotRodCache"
   protected var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private var advancedCache: AdvancedCache[ByteArrayKey, CacheValue] = _
   private var hotRodJmxDomain = getClass.getSimpleName

   override def createCacheManager: EmbeddedCacheManager = {
      val cacheManager = createTestCacheManager
      cacheManager.defineConfiguration(cacheName, cacheManager.getDefaultConfiguration)
      advancedCache = cacheManager.getCache[ByteArrayKey, CacheValue](cacheName).getAdvancedCache
      cacheManager
   }

   @Test(enabled = false)
   protected override def setup() {
      super.setup()
      hotRodServer = createStartHotRodServer(cacheManager)
      hotRodClient = connectClient
   }

   protected def createTestCacheManager: EmbeddedCacheManager = TestCacheManagerFactory.createLocalCacheManager(true)

   protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager)

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      log.debug("Test finished, close cache, client and Hot Rod server")
      super.destroyAfterClass
      shutdownClient
      killServer(hotRodServer)
   }

   protected def server = hotRodServer

   protected def client = hotRodClient

   protected def jmxDomain = hotRodJmxDomain

   protected def shutdownClient: ChannelFuture = killClient(hotRodClient)

   protected def connectClient: HotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 10)
}
