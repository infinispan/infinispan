package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleServerAuthenticationProvider;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author vjuranek
 * @since 9.0
 */
public abstract class AbstractAuthenticationTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(AuthenticationTest.class);
   protected RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;

   @Override
   protected abstract EmbeddedCacheManager createCacheManager() throws Exception;

   protected abstract SimpleServerAuthenticationProvider createAuthenticationProvider();

   protected ConfigurationBuilder initServerAndClient() {
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = HotRodTestingUtil.getDefaultHotRodConfiguration();
      serverBuilder.authentication()
         .enable()
         .serverName("localhost")
         .addAllowedMech("CRAM-MD5")
         .serverAuthenticationProvider(createAuthenticationProvider());
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

   @Override
   protected void teardown() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      super.teardown();
   }

}
