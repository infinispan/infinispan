package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.nio.channels.ClosedChannelException;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SslConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.security.TestCertificates;
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

/**
 * @author Adrian Brock
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(testName = "client.hotrod.SslTest", groups = "functional")
@CleanupAfterMethod
public class SslTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(SslTest.class);

   RemoteCache<String, String> defaultRemote;
   protected RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.getCache();

      return cacheManager;
   }

   protected void initServerAndClient(boolean sslServer, boolean sslClient, boolean requireClientAuth, boolean clientAuth, boolean usePEM) {
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();

      org.infinispan.server.core.configuration.SslConfigurationBuilder serverSSLConfig = serverBuilder.ssl()
            .enabled(sslServer)
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD)
            .keyStoreType(TestCertificates.KEYSTORE_TYPE);
      if (requireClientAuth) {
         serverSSLConfig
               .requireClientAuth(true)
               .trustStoreFileName(TestCertificates.certificate("trust"))
               .trustStoreType(TestCertificates.KEYSTORE_TYPE)
               .trustStorePassword(TestCertificates.KEY_PASSWORD);
      }
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, serverBuilder);
      log.info("Started server on port: " + hotrodServer.getPort());

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
               clientSSLConfig
                     .trustStoreFileName(TestCertificates.pem("ca"))
                     .trustStoreType("PEM");
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } else {
            clientSSLConfig
                  .trustStoreFileName(TestCertificates.certificate("ca"))
                  .trustStorePassword(TestCertificates.KEY_PASSWORD)
                  .trustStoreType(TestCertificates.KEYSTORE_TYPE);
         }
         if (clientAuth) {
            clientSSLConfig
                  .keyStoreFileName(TestCertificates.certificate("client"))
                  .keyStorePassword(TestCertificates.KEY_PASSWORD)
                  .keyStoreType(TestCertificates.KEYSTORE_TYPE);
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
      initServerAndClient(true, true, false, false, false);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   public void testSSLServerSSLClientWithPEM() throws Exception {
      initServerAndClient(true, true, false, false, true);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   @Test(expectedExceptions = TransportException.class )
   public void testSSLServerPlainClient() throws Exception {
      // The server just disconnects the client
      initServerAndClient(true, false, false, false, false);
   }

   @Test(expectedExceptions = TransportException.class )
   public void testPlainServerSSLClient() throws Exception {
      initServerAndClient(false, true, false, false, false);
   }

   public void testClientAuth() throws Exception {
      initServerAndClient(true, true, true, true, false);
      defaultRemote.put("k", "v");
      assertEquals("v", defaultRemote.get("k"));
   }

   // Note: with Netty this started to throw SSLException instead of SSLHandshakeException
   public void testClientAuthWithAnonClient() {
      Exceptions.expectExceptionNonStrict(TransportException.class, ClosedChannelException.class, () -> initServerAndClient(true, true, true, false, false));
   }
}
