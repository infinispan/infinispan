package org.infinispan.client.hotrod.test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * @author Galder Zamarreño
 */
public abstract class SingleHotRodServerTest extends SingleCacheManagerTest {

   protected HotRodServer hotrodServer;
   protected RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(contextInitializer(), hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = createHotRodServer();
      remoteCacheManager = getRemoteCacheManager();
      remoteCacheManager.getCache(); // start the cache
   }

   protected HotRodServer createHotRodServer() {
      return HotRodClientTestingUtil.startHotRodServer(cacheManager);
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = createHotRodClientConfigurationBuilder("127.0.0.1", hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   protected ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer()
            .host(host)
            .port(serverPort);
      SerializationContextInitializer sci = contextInitializer();
      if (sci != null)
         builder.addContextInitializer(sci);
      return builder;
   }

   protected SerializationContextInitializer contextInitializer() {
      return null;
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      hotrodServer = null;
      remoteCacheManager = null;

      super.teardown();
   }

}
