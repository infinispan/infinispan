package org.infinispan.client.hotrod.test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
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
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
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
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
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
