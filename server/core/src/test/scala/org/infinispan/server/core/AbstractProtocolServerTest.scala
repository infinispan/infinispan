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

      override def startInternal(configuration: MockServerConfiguration, cacheManager: EmbeddedCacheManager) {
         super.startInternal(configuration, cacheManager)
      }

      override def getEncoder = null

      override def getDecoder = null

      override def startTransport {
         this.tcpNoDelay = configuration.tcpNoDelay
      }
   }

}
