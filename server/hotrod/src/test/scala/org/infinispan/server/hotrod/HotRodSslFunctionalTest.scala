package org.infinispan.server.hotrod

import org.testng.annotations.Test
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.commons.util.SslContextFactory

/**
 * Hot Rod server functional test over SSL
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSslFunctionalTest")
class HotRodSslFunctionalTest extends HotRodFunctionalTest {

   private val tccl = Thread.currentThread().getContextClassLoader
   private val keyStoreFileName = tccl.getResource("keystore.jks").getPath
   private val trustStoreFileName = tccl.getResource("truststore.jks").getPath

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = {
      val builder = new HotRodServerConfigurationBuilder
      builder.proxyHost(host).proxyPort(UniquePortThreadLocal.get.intValue).idleTimeout(0)
      builder.ssl.enable().keyStoreFileName(keyStoreFileName).keyStorePassword("secret".toCharArray).trustStoreFileName(trustStoreFileName).trustStorePassword("secret".toCharArray)
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, -1, builder)
   }

   override protected def connectClient: HotRodClient = {
      val ssl = hotRodServer.getConfiguration.ssl
      val sslContext = SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.trustStoreFileName, ssl.trustStorePassword)
      val sslEngine = SslContextFactory.getEngine(sslContext, true, false)
      new HotRodClient(host, hotRodServer.getPort, cacheName, 60, 20, sslEngine)
   }
}
