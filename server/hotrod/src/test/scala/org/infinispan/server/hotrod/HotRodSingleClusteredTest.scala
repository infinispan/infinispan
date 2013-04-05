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
import org.testng.annotations.{AfterClass, BeforeClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.CacheMode
import org.infinispan.util.ByteArrayEquivalence

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSingleClusteredTest")
class HotRodSingleClusteredTest extends MultipleCacheManagersTest {

   private var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private val cacheName = "HotRodCache"

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
      cacheManagers.add(cm)
      val builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      cm.defineConfiguration(cacheName, builder.build())
   }

   @BeforeClass(alwaysRun = true)
   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createBeforeClass() {
      super.createBeforeClass()
      hotRodServer = startHotRodServer(cacheManagers.get(0))
      hotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 10)
   }

   @AfterClass(alwaysRun = true)
   override def destroy() {
      log.debug("Test finished, close client, server, and cache managers")
      killClient(hotRodClient)
      killServer(hotRodServer)
      super.destroy()
   }

   def testPutGet(m: Method) {
      assertStatus(hotRodClient.put(k(m) , 0, 0, v(m)), Success)
      assertSuccess(hotRodClient.get(k(m), 0), v(m))
   }

   
}