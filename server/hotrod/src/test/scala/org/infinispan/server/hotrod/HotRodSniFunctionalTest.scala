package org.infinispan.server.hotrod

import java.lang.reflect.Method
import javax.net.ssl.{SNIHostName, SNIServerName, SSLContext, SSLEngine}

import org.infinispan.commons.util.SslContextFactory
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test._
import org.testng.Assert
import org.testng.annotations.{AfterMethod, Test}

import scala.collection.JavaConverters._

/**
  * Hot Rod server functional test for SNI
  *
  * @author Sebastian Łaskawiec
  * @since 9.0
  * @see https://tools.ietf.org/html/rfc6066#page-6
  */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSniFunctionalTest")
class HotRodSniFunctionalTest extends HotRodSingleNodeTest {

  private val defaultServerKeystore = getClass.getClassLoader.getResource("default_server_keystore.jks").getPath
  private val sniServerKeystore = getClass.getClassLoader.getResource("sni_server_keystore.jks").getPath
  private val noAuthorizedClientsServerKeystore = getClass.getClassLoader.getResource("no_trusted_clients_keystore.jks").getPath

  private val defaultTrustedClientTruststore = getClass.getClassLoader.getResource("default_client_truststore.jks").getPath
  private val sniTrustedClientTruststore = getClass.getClassLoader.getResource("sni_client_truststore.jks").getPath

  @AfterMethod
  def afterMethod(): Unit = {
    //HotRodSingleNodeTest assumes that we start/shutdown server once instead of per-test. We need to perform our own cleanup.
    killClient(hotRodClient).get()
    killServer(hotRodServer)
  }

  def testServerAndClientWithDefaultSslContext(m: Method): Unit = {
    //given
    hotRodServer = new HotrodServerBuilder()
      .addSniDomain("*", defaultServerKeystore, "secret", defaultTrustedClientTruststore, "secret")
      .build()

    hotRodClient = new HotrodClientBuilder(hotRodServer)
      .useSslConfiguration(defaultServerKeystore, "secret", defaultTrustedClientTruststore, "secret")
      .build()

    //when
    client.assertPut(m)

    //then
    assertSuccess(client.assertGet(m), v(m))
  }

  def testServerAndClientWithSniSslContext(m: Method): Unit = {
    //given
    hotRodServer = new HotrodServerBuilder()
      //this will reject all clients without SNI Domain specified
      .addSniDomain("*", noAuthorizedClientsServerKeystore, "secret", sniTrustedClientTruststore, "secret")
      //and here we allow only those with SNI specified
      .addSniDomain("sni", sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
      .build()

    hotRodClient = new HotrodClientBuilder(hotRodServer)
      .useSslConfiguration(sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
      .addSniDomain("sni")
      .build()

    //when
    client.assertPut(m)

    //then
    assertSuccess(client.assertGet(m), v(m))
  }

  def testServerWithNotMatchingDefaultAndClientWithSNI(m: Method): Unit = {
    //given
    hotRodServer = new HotrodServerBuilder()
      .addSniDomain("*", noAuthorizedClientsServerKeystore, "secret", sniTrustedClientTruststore, "secret")
      .build()

    hotRodClient = new HotrodClientBuilder(hotRodServer)
      .useSslConfiguration(sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
      .addSniDomain("sni")
      .build()

    //when
    val op = new Op(0xA0, 0x01, 20, client.defaultCacheName, k(m), 0, 0, v(m), 0, 0, 1, 0)
    val success = client.writeOp(op, false)

    //assert
    Assert.assertFalse(success)
  }

  //Server configuration needs to be performed per test
  protected override def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = null

  //Client configuration needs to be performed per test
  protected override def connectClient = null

  class HotrodClientBuilder(hotRodServer: HotRodServer) {

    var sslContext: SSLContext = null
    var sslEngine: SSLEngine = null

    def useSslConfiguration(keystoreFileName: String, keystorePassoword: String, truststoreFileName: String, truststorePassword: String): HotrodClientBuilder = {
      sslContext = SslContextFactory.getContext(keystoreFileName, keystorePassoword.toCharArray, truststoreFileName, truststorePassword.toCharArray)
      sslEngine = SslContextFactory.getEngine(sslContext, true, false)
      return this
    }

    def addSniDomain(sniNames: String*): HotrodClientBuilder = {
      if (sniNames.size > 0) {
        val sslParameters = sslEngine.getSSLParameters()
        val hosts: List[SNIServerName] = sniNames.map(s => new SNIHostName(s)).toList
        sslParameters.setServerNames(hosts.asJava)
        sslEngine.setSSLParameters(sslParameters)
      }
      return this
    }

    def build(): HotRodClient = {
      new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 20, sslEngine)
    }
  }

  class HotrodServerBuilder {

    val ip = "127.0.0.1"
    val builder = new HotRodServerConfigurationBuilder()
      .proxyHost("127.0.0.1")
      .proxyPort(UniquePortThreadLocal.get.intValue)
      .idleTimeout(0)

    def addSniDomain(domain: String, keystoreFileName: String, keystorePassoword: String, truststoreFileName: String, truststorePassword: String): HotrodServerBuilder = {
      builder.ssl.enable()
        .sniHostName(domain)
        .keyStoreFileName(keystoreFileName)
        .keyStorePassword(keystorePassoword.toCharArray)
        .trustStoreFileName(truststoreFileName)
        .trustStorePassword(truststorePassword.toCharArray)
      return this
    }

    def build(): HotRodServer = {
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, -1, builder)
    }
  }

}
