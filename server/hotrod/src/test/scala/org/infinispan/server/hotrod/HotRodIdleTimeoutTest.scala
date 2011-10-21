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

import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import test.{HotRodClient, UniquePortThreadLocal}
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Tests idle timeout logic in Hot Rod.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodIdleTimeoutTest")
class HotRodIdleTimeoutTest extends HotRodSingleNodeTest {

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) =
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, 5)

   override protected def connectClient = new HotRodClient("127.0.0.1", server.getPort, cacheName, 10, 10)

   def testSendPartialRequest(m: Method) {
      client.assertPut(m)
      val resp = client.executePartial(0xA0, 0x03, cacheName, k(m) , 0, 0, v(m), 0)
      assertNull(resp) // No response received within expected timeout.
      client.assertPutFail(m)
      shutdownClient
      
      val newClient = connectClient
      try {
         newClient.assertPut(m)
      } finally {
         shutdownClient
      }
   }

}