package org.infinispan.client.hotrod;

import java.util.Map;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;

/**
 * @author vjuranek
 * @since 9.0
 */
@CleanupAfterTest
public abstract class AbstractAuthenticationTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(AuthenticationTest.class);
   // Each method should open its own RemoteCacheManager
   protected RemoteCacheManager remoteCacheManager;
   // All the methods should use the same HotRodServer
   protected HotRodServer hotrodServer;

   @Override
   protected abstract EmbeddedCacheManager createCacheManager() throws Exception;

   protected abstract SimpleSaslAuthenticator createAuthenticationProvider();

   protected HotRodServer initServer(Map<String, String> mechProperties, int index) {
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();
      serverBuilder.authentication()
         .enable()
            .sasl()
               .serverName("localhost")
               .addAllowedMech("CRAM-MD5")
               .authenticator(createAuthenticationProvider());
      serverBuilder.authentication().sasl().mechProperties(mechProperties);
      int port = HotRodTestingUtil.serverPort() + index;
      HotRodServer server = HotRodTestingUtil.startHotRodServer(cacheManager, port, serverBuilder);
      log.info("Started server on port: " + server.getPort());
      return server;
   }

   protected ConfigurationBuilder newClientBuilder() {
      return newClientBuilder(0);
   }

   protected ConfigurationBuilder newClientBuilder(int index) {
      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
         .addServer()
         .host(hotrodServer.getHost())
         .port(hotrodServer.getPort())
         .socketTimeout(3000)
         .maxRetries(3)
         .connectionPool()
         .maxActive(1)
         .security()
         .authentication()
         .enable()
         .saslMechanism("CRAM-MD5")
         .connectionPool()
            .maxActive(1);
      return clientBuilder;
   }

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killServers(hotrodServer);
      hotrodServer = null;

      super.teardown();
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void destroyAfterMethod() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;

      super.destroyAfterMethod();
   }
}
