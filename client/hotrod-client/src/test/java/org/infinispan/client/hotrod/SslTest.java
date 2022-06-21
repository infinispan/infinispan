package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.wildfly.security.provider.util.ProviderUtil.INSTALLED_PROVIDERS;

import java.io.FileInputStream;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SslConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;
import org.wildfly.security.keystore.KeyStoreUtil;

/**
 * @author Adrian Brock
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(testName = "client.hotrod.SslTest", groups = "functional")
@CleanupAfterMethod
public class SslTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(SslTest.class);
   public static final char[] STORE_PASSWORD = "secret".toCharArray();
   public static final char[] ALT_CERTIFICATE_PASSWORD = "changeme".toCharArray();

   RemoteCache<String, String> defaultRemote;
   protected RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.getCache();

      return cacheManager;
   }

   protected void initServerAndClient(boolean sslServer, boolean sslClient, boolean requireClientAuth, boolean clientAuth, boolean altCertPassword, boolean usePEM) throws URISyntaxException {
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();

      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      Path serverKeyStore = Paths.get(tccl.getResource(altCertPassword ? "keystore_server.jks" : "keystore_server.p12").toURI());
      Path serverTrustStore = Paths.get(tccl.getResource("ca.p12").toURI());
      org.infinispan.server.core.configuration.SslConfigurationBuilder serverSSLConfig = serverBuilder.ssl()
            .enabled(sslServer)
            .keyStoreFileName(serverKeyStore.toString())
            .keyStorePassword(STORE_PASSWORD)
            .keyStoreType(altCertPassword ? "JCEKS" : "PKCS12");
      if (altCertPassword)
         serverSSLConfig.keyStoreCertificatePassword(ALT_CERTIFICATE_PASSWORD);
      if (requireClientAuth) {
         serverSSLConfig
               .requireClientAuth(true)
               .trustStoreFileName(serverTrustStore.toString())
               .trustStoreType("PKCS12")
               .trustStorePassword(STORE_PASSWORD);
      }
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, serverBuilder);
      log.info("Started server on port: " + hotrodServer.getPort());

      Path clientKeyStore = Paths.get(tccl.getResource(altCertPassword ? "keystore_client.jks" : "keystore_client.p12").toURI());
      Path clientTrustStore = Paths.get(tccl.getResource("ca.p12").toURI());
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      SslConfigurationBuilder clientSSLConfig = clientBuilder
            .addServer()
               .host("127.0.0.1")
               .port(hotrodServer.getPort())
            .socketTimeout(3000)
            .connectionPool()
               .maxActive(1)
            .security()
            .authentication()
               .disable()
            .ssl();
      if (sslClient) {
         if(usePEM) {
            try {
               Path pem = clientTrustStore.getParent().resolve("ca.pem");
               KeyStore keyStore = KeyStoreUtil.loadKeyStore(INSTALLED_PROVIDERS, null,
                     new FileInputStream(clientTrustStore.toFile()), clientTrustStore.toString(), STORE_PASSWORD);
               String alias = keyStore.aliases().nextElement();
               Certificate certificate = keyStore.getCertificate(alias);
               JcaPEMWriter writer = new JcaPEMWriter(Files.newBufferedWriter(pem));
               writer.writeObject(certificate);
               writer.close();
               clientSSLConfig
                     .trustStoreFileName(pem.toString())
                     .trustStoreType("PEM");
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } else {
            clientSSLConfig
                  .trustStoreFileName(clientTrustStore.toString())
                  .trustStorePassword(STORE_PASSWORD)
                  .trustStoreType("PKCS12");
         }
         if (clientAuth) {
            clientSSLConfig
                  .keyStoreFileName(clientKeyStore.toString())
                  .keyStorePassword(STORE_PASSWORD)
                  .keyStoreType(altCertPassword ? "JCEKS" : "PKCS12");
            if (altCertPassword) {
               clientSSLConfig
                     .keyStoreCertificatePassword(ALT_CERTIFICATE_PASSWORD);
            }
         }
      }
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      defaultRemote = remoteCacheManager.getCache();
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      HotRodClientTestingUtil.killServers(hotrodServer);
      hotrodServer = null;
      super.teardown();
   }

   public void testSSLServerSSLClient() throws Exception {
      initServerAndClient(true, true, false, false, false, false);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   public void testSSLServerSSLClientWithPEM() throws Exception {
      initServerAndClient(true, true, false, false, false, true);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   @Test(expectedExceptions = TransportException.class )
   public void testSSLServerPlainClient() throws Exception {
      // The server just disconnects the client
      initServerAndClient(true, false, false, false, false, false);
   }

   @Test(expectedExceptions = TransportException.class )
   public void testPlainServerSSLClient() throws Exception {
      initServerAndClient(false, true, false, false, false, false);
   }

   public void testClientAuth() throws Exception {
      initServerAndClient(true, true, true, true, false, false);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   // Note: with Netty this started to throw SSLException instead of SSLHandshakeException
   public void testClientAuthWithAnonClient() {
      Exceptions.expectExceptionNonStrict(TransportException.class, ClosedChannelException.class, () -> initServerAndClient(true, true, true, false, false, false));
   }

   public void testClientAuthAltCertPassword() throws Exception {
      initServerAndClient(true, true, true, true, true, false);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }
}
