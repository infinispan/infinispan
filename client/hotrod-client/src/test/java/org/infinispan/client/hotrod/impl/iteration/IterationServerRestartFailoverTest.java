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

@Test(groups = "functional", testName = "client.hotrod.iteration.IterationServerRestartFailoverTest")
public class IterationServerRestartFailoverTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false)));
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      // Explicitly uses `localhost` instead of the IP.
      // This will trigger an update in the topology any time the server restarts.
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      builder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      return new RemoteCacheManager(builder.build());
   }

   public void restartServerAndIterateCacheWithEntries() {
      int numberOfEntries = 10;
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      for (int i = 0; i < numberOfEntries; i++) {
         cache.put("key-" + i, "value-" + i);
      }

      // Calculating size with a stream just to trigger the iteration mechanism.
      try (Stream<Object> stream = cache.values().stream()) {
         // Just doing something to consume the stream.
         long size = stream.count();
         assertThat(size).isEqualTo(numberOfEntries);
      }

      // We restart the server now, to trigger the connection to fallback to localhost
      int port = hotrodServer.getPort();
      killServers(hotrodServer);

      // The list will revert to localhost, and it'll be utilised in the next operation.
      eventually(() -> {
         CacheInfo ci = Internals.dispatcher(remoteCacheManager).getCacheInfo(cache.getName());
         return ci.getServers().stream().anyMatch(s -> s.getHostString().equals("localhost"));
      });

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotrodServer = startHotRodServer(cacheManager, port, serverBuilder);

      // With the server up again, let's try iterating again.
      try (Stream<Object> stream = cache.values().stream()) {
         // Just doing something to consume the stream.
         long size = stream.count();
         assertThat(size).isEqualTo(numberOfEntries);
      }
   }
}
