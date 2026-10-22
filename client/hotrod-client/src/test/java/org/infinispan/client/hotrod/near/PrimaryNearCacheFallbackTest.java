package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheFallbackTest")
public class PrimaryNearCacheFallbackTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheConfig.clustering().hash().numOwners(1);

      addHotRodServer(hotRodCacheConfiguration(cacheConfig));

      GlobalConfigurationBuilder globalBuilder = defaultGlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      modifyGlobalConfiguration(globalBuilder);
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(globalBuilder, hotRodCacheConfiguration(cacheConfig));
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      int serverPort = ServerTestingUtil.findFreePort();
      int unreachableProxyPort = 1;
      serverBuilder.proxyHost("127.0.0.1").proxyPort(unreachableProxyPort);
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverPort, serverBuilder);
      servers.add(server);

      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      managers.forEach(RemoteCacheManager::stop);
      managers.clear();
      super.destroy();
   }

   private Integer findKeyForServer(InvalidatedNearRemoteCache<Integer, String> inrc, int serverIndex) {
      HotRodServer targetServer = server(serverIndex);
      java.net.SocketAddress targetAddress = java.net.InetSocketAddress.createUnresolved("127.0.0.1", targetServer.getAddress().getPort());
      for (int i = 0; i < 10000; i++) {
         if (java.util.Objects.equals(inrc.getPrimaryOwner(i), targetAddress)) {
            return i;
         }
      }
      throw new IllegalStateException("Could not find key for server " + targetServer);
   }

   public void testPartialPrimaryListenerSuccess() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(server(0).getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100);

      RemoteCacheManager clientManager = new RemoteCacheManager(clientBuilder.build());
      managers.add(clientManager);

      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      assertThat(clientCache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;

      Integer key0 = findKeyForServer(inrc, 0);
      Integer key1 = findKeyForServer(inrc, 1);

      // Operations on key0 (primary on reachable server 0) should be cached in near cache
      clientCache.put(key0, "val0");

      // Because server 0 is reachable, the client remains in primary mode!
      assertThat(inrc.isPrimaryMode()).isTrue();

      assertThat(clientCache.get(key0)).isEqualTo("val0");
      assertThat(inrc.getNearCacheService().get(key0)).isNotNull();

      // Invalidation works for key0 via another client
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder updatingClientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      updatingClientBuilder.addServer().host("127.0.0.1").port(server(0).getPort());
      updatingClientBuilder.clientIntelligence(ClientIntelligence.BASIC);

      RemoteCacheManager updatingClient = new RemoteCacheManager(updatingClientBuilder.build());
      try {
         updatingClient.getCache().put(key0, "val0-updated");
      } finally {
         updatingClient.stop();
      }
      eventually(() -> inrc.getNearCacheService().get(key0) == null);
      assertThat(clientCache.get(key0)).isEqualTo("val0-updated");

      // Key1 is primary on server 1 (which had an unreachable proxy port and is inactive).
      // Reading key1 should NOT populate near cache
      clientCache.get(key1);
      assertThat(inrc.getNearCacheService().get(key1)).isNull();
   }

   public void testFallbackWhenHotRodProtocol41() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(server(0).getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.version(org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_41);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100);

      RemoteCacheManager clientManager = new RemoteCacheManager(clientBuilder.build());
      managers.add(clientManager);

      RemoteCache<Integer, String> clientCache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) clientCache;

      // In Hot Rod 4.1, primaryMode is not supported -> falls back to cluster listener!
      assertThat(inrc.isPrimaryMode()).isFalse();

      clientCache.put(10, "val10");
      assertThat(clientCache.get(10)).isEqualTo("val10");
      assertThat(inrc.getNearCacheService().get(10)).isNotNull();
   }
}
