package org.infinispan.server.hotrod

import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import java.util.Arrays
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test._
import org.infinispan.test.TestingUtil.generateRandomString
import java.util.concurrent.TimeUnit
import org.infinispan.server.core.test.Stoppable
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.server.core.QueryFacade
import org.infinispan.AdvancedCache
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import javax.security.sasl.Sasl
import javax.security.sasl.SaslClient
import java.util.HashMap
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider
import org.testng.annotations.BeforeClass
import java.security.Provider
import org.jboss.sasl.JBossSaslProvider
import org.testng.annotations.AfterClass
import java.security.AccessController
import java.security.PrivilegedAction
import java.security.Security

/**
 * Hot Rod server authentication test.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodAuthenticationTest")
class HotRodAuthenticationTest extends HotRodSingleNodeTest {
   val jbossSaslProvider = new JBossSaslProvider();

   override def createStartHotRodServer(cacheManager: EmbeddedCacheManager): HotRodServer = {
      val ssap = new SimpleServerAuthenticationProvider
      ssap.addUser("user", "realm", "password".toCharArray(), null)
      val builder = new HotRodServerConfigurationBuilder
      builder.authentication().enable().addAllowedMech("CRAM-MD5").serverAuthenticationProvider(ssap).serverName("localhost")
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, 0, builder)
   }

   @Test(enabled=false)
   def testAuthMechList(m: Method) {
      val a = client.authMechList
      assertEquals(a.mechs.size, 1)
      assertTrue(a.mechs.contains("CRAM-MD5"))
   }

   def testAuth(m: Method) {
      val props = new HashMap[String, String]
      val sc = Sasl.createSaslClient(Array("CRAM-MD5"), null, "hotrod", "localhost", props, new TestCallbackHandler("user", "realm", "password".toCharArray()))
      val res = client.auth(sc)
      assertTrue(res.complete)
   }

}
