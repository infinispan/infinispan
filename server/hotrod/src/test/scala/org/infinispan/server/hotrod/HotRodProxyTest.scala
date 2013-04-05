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

import org.infinispan.manager.EmbeddedCacheManager
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.util.ByteArrayEquivalence

/**
 * Tests Hot Rod instances that are behind a proxy.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodProxyTest")
class HotRodProxyTest extends HotRodMultiNodeTest {

   private val proxyHost1 = "1.2.3.4"
   private val proxyHost2 = "2.3.4.5"
   private val proxyPort1 = 8123
   private val proxyPort2 = 9123

   override protected def cacheName: String = "hotRodProxy"

   override protected def createCacheConfig: ConfigurationBuilder = {
      val config = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      config.clustering().stateTransfer().fetchInMemoryState(true)
      config
   }

   override protected def protocolVersion = 10

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager) =
      startHotRodServer(cacheManager, proxyHost1, proxyPort1)

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) =
      startHotRodServer(cacheManager, port, proxyHost2, proxyPort2)

   def testTopologyWithProxiesReturned() {
      val resp = clients.head.ping(2, 0)
      assertStatus(resp, Success)
      val topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.topologyId, currentServerTopologyId)
      assertEquals(topoResp.members.size, 2)
      topoResp.members.foreach(member => servers.map(_.getAddress).exists(_ == member))
   }

}
