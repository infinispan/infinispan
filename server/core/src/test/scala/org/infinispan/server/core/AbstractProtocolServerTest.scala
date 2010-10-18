package org.infinispan.server.core

import org.testng.annotations.Test
import java.util.Properties
import transport.{Decoder, Encoder}
import org.infinispan.server.core.Main._
import org.infinispan.manager.{DefaultCacheManager, EmbeddedCacheManager}
import org.testng.Assert._
import java.net.InetSocketAddress
import java.lang.reflect.Method
import org.infinispan.util.TypedProperties

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

   def testHostPropertySubstitution(m: Method) {
      var host = "1.2.3.4";
      var p = new Properties
      p.setProperty(PROP_KEY_HOST, host);
      var server = createServer
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getHost, host)

      host = "${" + m.getName + "-myhost:5.6.7.8}"
      p = new Properties
      p.setProperty(PROP_KEY_HOST, host);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getHost, "5.6.7.8")

      host = "${" + m.getName + "-myhost:9.10.11.12}"
      System.setProperty(m.getName + "-myhost", "13.14.15.16");
      p = new Properties
      p.setProperty(PROP_KEY_HOST, host);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getHost, "13.14.15.16")

      host = "${" + m.getName + "-otherhost}"
      p = new Properties
      p.setProperty(PROP_KEY_HOST, host);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getHost, host)

      host = "${" + m.getName + "-otherhost}"
      System.setProperty(m.getName + "-otherhost", "17.18.19.20");
      p = new Properties
      p.setProperty(PROP_KEY_HOST, host);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getHost, "17.18.19.20")
   }

   def testPortPropertySubstitution(m: Method) {
      var port = "123"
      var p = new Properties
      p.setProperty(PROP_KEY_PORT, port);
      var server = createServer
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getPort, port.toInt)

      port = "${" + m.getName + "-myport:567}"
      p = new Properties
      p.setProperty(PROP_KEY_PORT, port);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getPort, 567)

      port = "${" + m.getName + "-myport:891}"
      System.setProperty(m.getName + "-myport", "234");
      p = new Properties
      p.setProperty(PROP_KEY_PORT, port);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getPort, 234)

      port = "${" + m.getName + "-otherport}"
      p = new Properties
      p.setProperty(PROP_KEY_PORT, port);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getPort, 12345)

      port = "${" + m.getName + "-otherport}"
      System.setProperty(m.getName + "-otherport", "5567");
      p = new Properties
      p.setProperty(PROP_KEY_PORT, port);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.getPort, 5567)
   }

   def testTcpNoDelayPropertySubstitution(m: Method) {
      var tcpNoDelay = "true"
      var p = new Properties
      p.setProperty(PROP_KEY_TCP_NO_DELAY, tcpNoDelay);
      var server = createServer
      server.start(p, new DefaultCacheManager)
      assertEquals(server.tcpNoDelay, tcpNoDelay.toBoolean)

      tcpNoDelay = "${" + m.getName + "-mytcpnodelay:false}"
      p = new Properties
      p.setProperty(PROP_KEY_TCP_NO_DELAY, tcpNoDelay);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.tcpNoDelay, false)

      tcpNoDelay = "${" + m.getName + "-mytcpnodelay:true}"
      System.setProperty(m.getName + "-mytcpnodelay", "false");
      p = new Properties
      p.setProperty(PROP_KEY_TCP_NO_DELAY, tcpNoDelay);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.tcpNoDelay, false)

      tcpNoDelay = "${" + m.getName + "-othertcpnodelay}"
      p = new Properties
      p.setProperty(PROP_KEY_TCP_NO_DELAY, tcpNoDelay);
      server.start(p, new DefaultCacheManager)
      // Boolean.parseBoolean() returning false to anything other than true, no exception thrown
      assertEquals(server.tcpNoDelay, false)

      tcpNoDelay = "${" + m.getName + "-othertcpnodelay}"
      System.setProperty(m.getName + "-othertcpnodelay", "true");
      p = new Properties
      p.setProperty(PROP_KEY_PORT, tcpNoDelay);
      server.start(p, new DefaultCacheManager)
      assertEquals(server.tcpNoDelay, true)
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

   private def createServer : MockProtocolServer = new MockProtocolServer

   class MockProtocolServer extends AbstractProtocolServer("Mock") {
      var tcpNoDelay: Boolean = _

      override def start(properties: Properties, cacheManager: EmbeddedCacheManager) {
         super.start(properties, cacheManager, 12345)
      }

      override def getEncoder: Encoder = null

      override def getDecoder: Decoder = null

      override def startTransport(idleTimeout: Int, tcpNoDelay: Boolean, sendBufSize: Int, recvBufSize: Int,
                                  typedProps: TypedProperties) {
         this.tcpNoDelay = tcpNoDelay
      }
   }

}