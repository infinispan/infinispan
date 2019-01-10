package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.Op;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Hot Rod server functional test for SNI.
 *
 * See <a href="https://tools.ietf.org/html/rfc6066#page-6">RFC 6066</a>.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodSniFunctionalTest")
public class HotRodSniFunctionalTest extends HotRodSingleNodeTest {

   private String defaultServerKeystore =
         getClass().getClassLoader().getResource("default_server_keystore.p12").getPath();
   private String sniServerKeystore = getClass().getClassLoader().getResource("sni_server_keystore.p12").getPath();
   private String noAuthorizedClientsServerKeystore =
         getClass().getClassLoader().getResource("no_trusted_clients_keystore.p12").getPath();

   private String defaultTrustedClientTruststore =
         getClass().getClassLoader().getResource("default_client_truststore.p12").getPath();
   private String sniTrustedClientTruststore =
         getClass().getClassLoader().getResource("sni_client_truststore.p12").getPath();

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      //HotRodSingleNodeTest assumes that we start/shutdown server once instead of per-test. We need to perform our own cleanup.
      killClient(hotRodClient);
      killServer(hotRodServer);
   }

   public void testServerAndClientWithDefaultSslContext(Method m) {
      //given
      hotRodServer = new HotrodServerBuilder()
            .addSniDomain("*", defaultServerKeystore, "secret", defaultTrustedClientTruststore, "secret")
            .build();

      hotRodClient = new HotrodClientBuilder(hotRodServer)
            .useSslConfiguration(defaultServerKeystore, "secret", defaultTrustedClientTruststore, "secret")
            .build();

      //when
      client().assertPut(m);

      //then
      assertSuccess(client().assertGet(m), v(m));
   }

   public void testServerAndClientWithSniSslContext(Method m) {
      //given
      hotRodServer = new HotrodServerBuilder()
            //this will reject all clients without SNI Domain specified
            .addSniDomain("*", noAuthorizedClientsServerKeystore, "secret", sniTrustedClientTruststore, "secret")
            //and here we allow only those with SNI specified
            .addSniDomain("sni", sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
            .build();

      hotRodClient = new HotrodClientBuilder(hotRodServer)
            .useSslConfiguration(sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
            .addSniDomain(Collections.singletonList("sni"))
            .build();

      //when
      client().assertPut(m);

      //then
      assertSuccess(client().assertGet(m), v(m));
   }

   public void testServerWithNotMatchingDefaultAndClientWithSNI(Method m) {
      //given
      hotRodServer = new HotrodServerBuilder()
            .addSniDomain("*", noAuthorizedClientsServerKeystore, "secret", sniTrustedClientTruststore, "secret")
            .build();

      hotRodClient = new HotrodClientBuilder(hotRodServer)
            .useSslConfiguration(sniServerKeystore, "secret", sniTrustedClientTruststore, "secret")
            .addSniDomain(Collections.singletonList("sni"))
            .build();

      //when
      Op op = new Op(0xA0, (byte) 0x01, (byte) 20, client().defaultCacheName(), k(m), 0, 0, v(m), 0, 0, (byte) 1, 0);
      boolean success = client().writeOp(op, false);

      //assert
      Assert.assertFalse(success);
   }

   //Server configuration needs to be performed per test
   protected @Override
   HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return null;
   }

   //Client configuration needs to be performed per test
   protected @Override
   HotRodClient connectClient() {
      return null;
   }

   class HotrodClientBuilder {

      private final HotRodServer hotRodServer;
      SSLContext sslContext;
      SSLEngine sslEngine;

      public HotrodClientBuilder(HotRodServer hotRodServer) {
         this.hotRodServer = hotRodServer;
      }

      public HotrodClientBuilder useSslConfiguration(String keystoreFileName, String keystorePassword,
                                                     String truststoreFileName, String truststorePassword) {
         sslContext = SslContextFactory.getContext(keystoreFileName, "pkcs12", keystorePassword.toCharArray(),
               truststoreFileName, "pkcs12", truststorePassword.toCharArray());
         sslEngine = SslContextFactory.getEngine(sslContext, true, false);
         return this;
      }

      public HotrodClientBuilder addSniDomain(List<String> sniNames) {
         if (!sniNames.isEmpty()) {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            List<SNIServerName> hosts = sniNames.stream().map(SNIHostName::new).collect(Collectors.toList());
            sslParameters.setServerNames(hosts);
            sslEngine.setSSLParameters(sslParameters);
         }
         return this;
      }

      public HotRodClient build() {
         return new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, (byte) 20, sslEngine);
      }
   }

   class HotrodServerBuilder {

      String ip = "127.0.0.1";
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder()
            .proxyHost("127.0.0.1")
            .proxyPort(serverPort())
            .idleTimeout(0);

      public HotrodServerBuilder addSniDomain(String domain, String keystoreFileName, String keystorePassword,
                                              String truststoreFileName, String truststorePassword) {
         builder.ssl().enable()
                .sniHostName(domain)
                .keyStoreFileName(keystoreFileName)
                .keyStorePassword(keystorePassword.toCharArray())
                .keyStoreType("pkcs12")
                .trustStoreFileName(truststoreFileName)
                .trustStorePassword(truststorePassword.toCharArray())
                .trustStoreType("pkcs12");
         return this;
      }

      public HotRodServer build() {
         return startHotRodServer(cacheManager, serverPort(), -1, builder);
      }
   }

}
