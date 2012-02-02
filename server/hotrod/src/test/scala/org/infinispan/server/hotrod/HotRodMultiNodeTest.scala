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

import org.infinispan.config.Configuration
import org.testng.annotations.{AfterMethod, AfterClass, Test}
import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.{TestingUtil, MultipleCacheManagersTest}

/**
 * Base test class for multi node or clustered Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodMultiNodeTest extends MultipleCacheManagersTest {
   private[this] var hotRodServers: List[HotRodServer] = List()
   private[this] var hotRodClients: List[HotRodClient] = List()

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers {
      for (i <- 0 until 2) {
         val cm = super.addClusterEnabledCacheManager()
         cm.defineConfiguration(cacheName, createCacheConfig)
      }
      hotRodServers = hotRodServers ::: List(startTestHotRodServer(cacheManagers.get(0)))
      hotRodServers = hotRodServers ::: List(startTestHotRodServer(cacheManagers.get(1), hotRodServers.head.getPort + 50))
      hotRodServers.foreach {s =>
         hotRodClients = new HotRodClient(
            "127.0.0.1", s.getPort, cacheName, 60, protocolVersion) :: hotRodClients
      }
   }

   protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager)

   protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) = startHotRodServer(cacheManager, port)

   protected def startClusteredServer(port: Int): HotRodServer =
      startClusteredServer(port, false)

   protected def startClusteredServer(port: Int, doCrash: Boolean): HotRodServer = {
      val cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, createCacheConfig)

      val newServer =
         try {
            if (doCrash) startCrashingHotRodServer(cm, port)
            else startHotRodServer(cm, port)
         } catch {
            case e: Exception => {
               log.error("Exception starting Hot Rod server", e)
               TestingUtil.killCacheManagers(cm)
               throw e
            }
         }

      TestingUtil.blockUntilViewsReceived(
         50000, true, cm, manager(0), manager(1))
      newServer
   }

   protected def stopClusteredServer(server: HotRodServer) {
      server.stop
      server.getCacheManager.stop
      TestingUtil.blockUntilViewsReceived(
         50000, false, manager(0), manager(1))
   }

   @AfterClass(alwaysRun = true)
   override def destroy {
      try {
         log.debug("Test finished, close Hot Rod server")
         hotRodClients.foreach(_.stop)
         hotRodServers.foreach(_.stop)
      } finally {
         super.destroy // Stop the caches last so that at stoppage time topology cache can be updated properly
      }
   }

   @AfterMethod(alwaysRun=true)
   override def clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   protected def servers = hotRodServers

   protected def clients = hotRodClients

   protected def cacheName: String

   protected def createCacheConfig: Configuration

   protected def protocolVersion: Byte
}
