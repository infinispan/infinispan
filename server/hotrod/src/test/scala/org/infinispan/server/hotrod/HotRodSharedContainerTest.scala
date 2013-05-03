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

import test.HotRodClient
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.MultipleCacheManagersTest
import test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._
import org.testng.annotations.{AfterClass, BeforeClass, AfterMethod, Test}
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.CacheMode
import org.infinispan.util.ByteArrayEquivalence
import org.infinispan.config.ConfigurationException
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.configuration.global.GlobalConfigurationBuilder
import org.infinispan.server.hotrod.test.UniquePortThreadLocal

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSharedContainerTest")
class HotRodSharedContainerTest extends MultipleCacheManagersTest {

   private var hotRodServer1: HotRodServer = _
   private var hotRodServer2: HotRodServer = _
   private var hotRodClient1: HotRodClient = _
   private var hotRodClient2: HotRodClient = _
   private val cacheName = "HotRodCache"

   @Test(enabled=false) // to avoid TestNG picking it up as a test
   override def createCacheManagers() {
      val globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder()
      globalCfg.globalJmxStatistics().allowDuplicateDomains(true)
      val cm = TestCacheManagerFactory.createClusteredCacheManager(globalCfg, hotRodCacheConfiguration())
      cacheManagers.add(cm)
      val builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      cm.defineConfiguration(cacheName, builder.build())
   }

   @Test(expectedExceptions= Array(classOf[ConfigurationException]))
   def testTopologyConflict() {
      val basePort = UniquePortThreadLocal.get.intValue
      hotRodServer1 = startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder())
      hotRodServer2 = startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder())
   }

   def testSharedContainer(m: Method) {
      val basePort = UniquePortThreadLocal.get.intValue
      hotRodServer1 = startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder().name("1"))
      hotRodServer2 = startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder().name("2"))

      hotRodClient1 = new HotRodClient("127.0.0.1", hotRodServer1.getPort, cacheName, 60, 12)
      hotRodClient2 = new HotRodClient("127.0.0.1", hotRodServer2.getPort, cacheName, 60, 12)

      hotRodClient1.put(k(m) , 0, 0, v(m))
      assertSuccess(hotRodClient2.get(k(m), 0), v(m))
   }

   @AfterMethod(alwaysRun = true)
   def killClientsAndServers() {
      killClient(hotRodClient1)
      killClient(hotRodClient2)
      killServer(hotRodServer1)
      killServer(hotRodServer2)
   }

}