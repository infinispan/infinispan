package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(testName = "client.hotrod.AuthenticationTest", groups = "functional")
@CleanupAfterMethod
public class AuthenticationTest extends SingleCacheManagerTest {
   private static final Log log = LogFactory.getLog(AuthenticationTest.class);
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.getCache();

      return cacheManager;
   }

   private ConfigurationBuilder initServerAndClient() {
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();
      SimpleServerAuthenticationProvider sap = new SimpleServerAuthenticationProvider();
      sap.addUser("user", "realm", "password".toCharArray(), null);
      serverBuilder.authentication()
         .enable()
         .serverName("localhost")
         .addAllowedMech("CRAM-MD5")
         .serverAuthenticationProvider(sap);
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
         .security()
            .authentication()
               .enable()
               .saslMechanism("CRAM-MD5")
          .connectionPool()
             .timeBetweenEvictionRuns(2000);
      return clientBuilder;
   }

   @Test
   public void testAuthentication() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<String, String> defaultRemote = remoteCacheManager.getCache();
      defaultRemote.put("a", "a");
      assertEquals("a", defaultRemote.get("a"));
   }

   @Test(expectedExceptions=TransportException.class)
   public void testAuthenticationFail() {
      ConfigurationBuilder clientBuilder = initServerAndClient();
      clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "foobar".toCharArray()));
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCacheManager.getCache();
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      super.teardown();
   }


}
