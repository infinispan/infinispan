package org.infinispan.client.hotrod.impl.iteration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.stream.Stream;

import org.infinispan.client.hotrod.Internals;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.iteration.IterationInitialServersRetryTest")
public class IterationInitialServersRetryTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false)));
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      // Include a bad server in the address list to easily reproduce a failure when establishing a connection.
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort() + 1024);
      builder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      return new RemoteCacheManager(builder.build());
   }

   public void restartServerAndIterate() {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      try (Stream<Object> stream = cache.values().stream()) {
         long size = stream.count();
         assertThat(size).isZero();
      }

      // We stop the server to return to the configured server list.
      int port = hotrodServer.getPort();
      killServers(hotrodServer);

      // Since the server was closed, we wait until the cache information fallback to the initial list that contains the
      // bad server.
      long badPort = port + 1024;
      eventually(() -> {
         CacheInfo ci = Internals.dispatcher(remoteCacheManager).getCacheInfo(cache.getName());
         return ci.getServers().stream().anyMatch(s -> s.getPort() == badPort);
      });

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotrodServer = startHotRodServer(cacheManager, port, serverBuilder);

      // With the server up again, let's try iterating again.
      // This one will hit the bad server when trying to establish a connection.
      try (Stream<Object> stream = cache.values().stream()) {
         // Just doing something to consume the stream.
         long size = stream.count();
         assertThat(size).isZero();
      }
   }
}
