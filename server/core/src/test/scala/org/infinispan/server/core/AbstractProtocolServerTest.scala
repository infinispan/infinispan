package org.infinispan.server.core

import org.testng.annotations.Test
import java.util.Properties
import transport.{Decoder, Encoder}
import org.infinispan.server.core.Main._
import org.infinispan.manager.{DefaultCacheManager, EmbeddedCacheManager}

/**
 * Abstract protocol server test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.AbstractProtocolServerTest")
class AbstractProtocolServerTest {

   def testValidateNegativeMasterThreads {
      val p = new Properties
      p.setProperty(PROP_KEY_MASTER_THREADS, "-1")
      expectIllegalArgument(p, createServer)
   }

   def testValidateNegativeWorkerThreads {
      val p = new Properties
      p.setProperty(PROP_KEY_WORKER_THREADS, "-1")
      expectIllegalArgument(p, createServer)
   }

   def testValidateNegativeIdleTimeout {
      val p = new Properties
      p.setProperty(PROP_KEY_IDLE_TIMEOUT, "-1")
      expectIllegalArgument(p, createServer)
   }

   def testValidateIllegalTcpNoDelay {
      val p = new Properties
      p.setProperty(PROP_KEY_TCP_NO_DELAY, "blah")
      expectIllegalArgument(p, createServer)
   }

   def testValidateNegativeSendBufSize {
      val p = new Properties
      p.setProperty(PROP_KEY_SEND_BUF_SIZE, "-1")
      expectIllegalArgument(p, createServer)
   }

   def testValidateNegativeRecvBufSize {
      val p = new Properties
      p.setProperty(PROP_KEY_RECV_BUF_SIZE, "-1")
      expectIllegalArgument(p, createServer)
   }

   private def expectIllegalArgument(p: Properties, server: ProtocolServer) {
      try {
         server.start(p, new DefaultCacheManager)
      } catch {
         case i: IllegalArgumentException => // expected
      } finally {
         server.stop
      }
   }

   private def createServer : ProtocolServer = {
      new AbstractProtocolServer("MyPrefix") {
         override def start(properties: Properties, cacheManager: EmbeddedCacheManager) {
            super.start(properties, cacheManager, 12345)
         }

         override def getEncoder: Encoder = null

         override def getDecoder: Decoder = null
      }
   }

}