/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.server.memcached

import org.infinispan.test.MultipleCacheManagersTest
import test.MemcachedTestingUtil._
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{AfterClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil
import org.infinispan.manager.EmbeddedCacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class MemcachedMultiNodeTest extends MultipleCacheManagersTest {

   private val cacheName = "MemcachedReplSync"
   var servers: List[MemcachedServer] = List()
   var clients: List[MemcachedClient] = List()
   val timeout: Int = 60

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      (0 until 2).foreach { i =>
         cacheManagers.add(createCacheManager(i))
      }

      waitForClusterToForm()
      servers = startMemcachedTextServer(cacheManagers.get(0), cacheName) :: servers
      servers = startMemcachedTextServer(cacheManagers.get(1), servers.head.getPort + 50, cacheName) :: servers
      servers.foreach(s => clients = createMemcachedClient(60000, s.getPort) :: clients)
   }

   protected def createCacheManager(index: Int): EmbeddedCacheManager

   @AfterClass(alwaysRun = true)
   override def destroy() {
      super.destroy()
      log.debug("Test finished, close Hot Rod server")
      clients.foreach(killMemcachedClient(_))
      servers.foreach(ServerTestingUtil.killServer(_))
   }

}
