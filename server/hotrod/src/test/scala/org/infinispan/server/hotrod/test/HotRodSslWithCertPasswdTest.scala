package org.infinispan.server.hotrod.test

import org.infinispan.server.core.test.Stoppable
import org.infinispan.server.hotrod.HotRodServer
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory._
import org.testng.Assert._
import org.testng.annotations.Test

/**
  * Tests HotRod server start with SSL enabled and keystore which has different keystore and certificate passwords.
  *
  * @author vjuranek
  * @since 9.0
  */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSslWithCertPasswdTest")
class HotRodSslWithCertPasswdTest {

   private val tccl = Thread.currentThread().getContextClassLoader
   private val keyStoreFileName = tccl.getResource("keystore2.jks").getPath
   private val trustStoreFileName = tccl.getResource("truststore.jks").getPath

   def testServerStartWithSslAndCertPasswd() {
      val builder = new HotRodServerConfigurationBuilder
      builder.proxyHost(host).proxyPort(UniquePortThreadLocal.get.intValue).idleTimeout(0)
      builder.ssl.enable().keyStoreFileName(keyStoreFileName).keyStorePassword("secret".toCharArray).keyStoreCertificatePassword("changeme".toCharArray).trustStoreFileName(trustStoreFileName).trustStorePassword("secret".toCharArray)
      Stoppable.useCacheManager(createCacheManager(hotRodCacheConfiguration())) { cm =>
         Stoppable.useServer(new HotRodServer) { server =>
            server.start(builder.build, cm)
            assertNotNull(server.getConfiguration.ssl().keyStoreCertificatePassword())
         }
      }
   }

}
