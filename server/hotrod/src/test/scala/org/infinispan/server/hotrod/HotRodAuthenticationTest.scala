package org.infinispan.server.hotrod

import java.lang.reflect.Method
import java.util.HashMap
import javax.security.sasl.Sasl

import io.netty.channel.group.ChannelGroup
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer
import org.infinispan.server.hotrod.test.{HotRodTestingUtils, TestCallbackHandler}
import org.infinispan.test.TestingUtil
import org.jboss.sasl.JBossSaslProvider
import org.testng.AssertJUnit.{assertEquals, assertTrue}
import org.testng.annotations.Test

/**
 * Hot Rod server authentication test.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodAuthenticationTest")
class HotRodAuthenticationTest extends HotRodSingleNodeTest {
   val jbossSaslProvider = new JBossSaslProvider()

   override def createStartHotRodServer(cacheManager: EmbeddedCacheManager): HotRodServer = {
      val ssap = new SimpleServerAuthenticationProvider
      ssap.addUser("user", "realm", "password".toCharArray, null)
      val builder = new HotRodServerConfigurationBuilder
      builder.authentication().enable().addAllowedMech("CRAM-MD5").serverAuthenticationProvider(ssap).serverName("localhost").addMechProperty(Sasl.POLICY_NOANONYMOUS, "true")
      startHotRodServer(cacheManager, HotRodTestingUtils.serverPort, 0, builder)
   }

   def testAuthMechList(m: Method) {
      val a = client.authMechList
      assertEquals(1, a.mechs.size)
      assertTrue(a.mechs.contains("CRAM-MD5"))
      assertEquals(1, server.getDecoder.getTransport.getNumberOfLocalConnections)
   }

   def testAuth(m: Method) {
      val props = new HashMap[String, String]
      val sc = Sasl.createSaslClient(Array("CRAM-MD5"), null, "hotrod", "localhost", props, new TestCallbackHandler("user", "realm", "password".toCharArray))
      val res = client.auth(sc)
      assertTrue(res.complete)
      assertEquals(1, server.getDecoder.getTransport.getNumberOfLocalConnections)
   }
   
   def testUnauthorizedOpCloseConnection(m: Method) {
      // Ensure the transport is clean
      val acceptedChannels : ChannelGroup = TestingUtil.extractField(server.getDecoder.getTransport, "acceptedChannels")
      acceptedChannels.close().awaitUninterruptibly()
      try {
        client.assertPutFail(m)
      } finally {
         assertEquals(0, server.getDecoder.getTransport.getNumberOfLocalConnections)
      }
   }

}
