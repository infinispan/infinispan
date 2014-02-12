package org.infinispan.client.hotrod;

import java.net.SocketTimeoutException;

import javax.net.ssl.SSLException;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * @author Adrian Brock
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(testName = "client.hotrod.SslTest", groups = "functional")
public class SslTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(SslTest.class);

   RemoteCache<String, String> defaultRemote;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.getCache();

      return cacheManager;
   }

   private void initServerAndClient(boolean sslServer, boolean sslClient) {
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();

      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      String keyStoreFileName = tccl.getResource("keystore.jks").getPath();
      String trustStoreFileName = tccl.getResource("truststore.jks").getPath();
      serverBuilder.ssl()
         .enabled(sslServer)
         .keyStoreFileName(keyStoreFileName)
         .keyStorePassword("secret".toCharArray())
         .trustStoreFileName(trustStoreFileName)
         .trustStorePassword("secret".toCharArray());
      hotrodServer.start(serverBuilder.build(), cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder
         .addServer()
            .host("127.0.0.1")
            .port(hotrodServer.getPort())
            .socketTimeout(3000)
         .connectionPool()
            .maxActive(1)
         .ssl()
            .enabled(sslClient)
            .keyStoreFileName(keyStoreFileName)
            .keyStorePassword("secret".toCharArray())
            .trustStoreFileName(trustStoreFileName)
            .trustStorePassword("secret".toCharArray())
          .connectionPool()
             .timeBetweenEvictionRuns(2000);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      defaultRemote = remoteCacheManager.getCache();
   }

   @AfterMethod
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   public void testSSLServerSSLClient() throws Exception {
      initServerAndClient(true, true);
      defaultRemote.put("k","v");
      assert defaultRemote.get("k").equals("v");
   }

   @Test(expectedExceptions = TransportException.class)
   public void testSSLServerPlainClient() throws Exception {
      // The server just disconnect the client
      initServerAndClient(true, false);
   }

   public void testPlainServerSSLClient() throws Exception {
      try {
         initServerAndClient(false, true);
         fail("Expecting a SSLException");
      } catch (TransportException e) {
          assertTrue(e.getCause() instanceof SSLException);
      }
   }
}
