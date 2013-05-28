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
package org.infinispan.server.core

import org.testng.annotations.Test
import org.infinispan.manager.EmbeddedCacheManager
import org.testng.Assert._
import java.lang.reflect.Method
import test.Stoppable
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.server.core.configuration.MockServerConfigurationBuilder
import org.infinispan.server.core.configuration.MockServerConfiguration

/**
 * Abstract protocol server test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.AbstractProtocolServerTest")
class AbstractProtocolServerTest {

   def testValidateNegativeWorkerThreads() {
      val b = new MockServerConfigurationBuilder
      b.workerThreads(-1);
      expectIllegalArgument(b, createServer)
   }

   def testValidateNegativeIdleTimeout() {
      val b = new MockServerConfigurationBuilder
      b.idleTimeout(-2);
      expectIllegalArgument(b, createServer)
   }

   def testValidateNegativeSendBufSize() {
      val b = new MockServerConfigurationBuilder
      b.sendBufSize(-1);
      expectIllegalArgument(b, createServer)
   }

   def testValidateNegativeRecvBufSize() {
      val b = new MockServerConfigurationBuilder
      b.recvBufSize(-1);
      expectIllegalArgument(b, createServer)
   }

   private def expectIllegalArgument(builder: MockServerConfigurationBuilder, server: MockProtocolServer) {
      try {
         Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager) { cm =>
            server.start(builder.build(), cm)
         }
      } catch {
         case i: IllegalArgumentException => // expected
      } finally {
         server.stop
      }
   }

   private def createServer : MockProtocolServer = new MockProtocolServer

   class MockProtocolServer extends AbstractProtocolServer("Mock") {
      type SuitableConfiguration = MockServerConfiguration

      var tcpNoDelay: Boolean = _

      override def start(configuration: MockServerConfiguration, cacheManager: EmbeddedCacheManager) {
         super.start(configuration, cacheManager)
      }

      override def getEncoder = null

      override def getDecoder = null

      override def startTransport {
         this.tcpNoDelay = configuration.tcpNoDelay
      }
   }

}
