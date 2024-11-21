package org.infinispan.client.hotrod.xsite;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.newRemoteConfigurationBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.Internals;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ClusterConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ServerConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests independent {@link ClientIntelligence} per cluster.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "client.hotrod.xsite.ClientIntelligenceClusterTest")
public class ClientIntelligenceClusterTest extends AbstractMultipleSitesTest {

   private final Map<String, List<HotRodServer>> siteServers = new HashMap<>(defaultNumberOfSites());

   @Override
   protected void afterSitesCreated() {
      super.afterSitesCreated();
      for (TestSite site : sites) {
         siteServers.put(site.getSiteName(), site.cacheManagers().stream().map(HotRodClientTestingUtil::startHotRodServer).collect(Collectors.toList()));
      }
   }

   @Override
   protected void killSites() {
      siteServers.values().forEach(hotRodServers -> hotRodServers.forEach(HotRodClientTestingUtil::killServers));
      siteServers.clear();
      super.killSites();
   }

   public void testClusterInheritsIntelligence() {
      try (RemoteCacheManager irc = createClient(ClientIntelligence.BASIC, null)) {
         RemoteCache<String, String> cache0 = irc.getCache();
         String cacheName = cache0.getName();
         cache0.put("key", "value");
         AssertJUnit.assertEquals("value", cache0.get("key"));
         assertBasicIntelligence(irc, cacheName);

         irc.switchToCluster("backup");
         cache0.put("key1", "value1");
         AssertJUnit.assertEquals("value1", cache0.get("key1"));
         assertBasicIntelligence(irc, cacheName);

         irc.switchToDefaultCluster();
         cache0.put("key2", "value2");
         AssertJUnit.assertEquals("value2", cache0.get("key2"));
         assertBasicIntelligence(irc, cacheName);
      }
   }

   public void testBackupClusterUsesBasicIntelligence() {
      try (RemoteCacheManager irc = createClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE, ClientIntelligence.BASIC)) {
         RemoteCache<String, String> cache0 = irc.getCache();
         String cacheName = cache0.getName();
         cache0.put("key", "value");
         AssertJUnit.assertEquals("value", cache0.get("key"));
         assertHashAwareIntelligence(irc, cacheName);

         irc.switchToCluster("backup");
         cache0.put("key1", "value1");
         AssertJUnit.assertEquals("value1", cache0.get("key1"));
         assertBasicIntelligence(irc, cacheName);

         irc.switchToDefaultCluster();
         cache0.put("key2", "value2");
         AssertJUnit.assertEquals("value2", cache0.get("key2"));
         assertHashAwareIntelligence(irc, cacheName);
      }
   }

   public void testBackupClusterUsesTopologyIntelligence() {
      try (RemoteCacheManager irc = createClient(ClientIntelligence.BASIC, ClientIntelligence.TOPOLOGY_AWARE)) {
         RemoteCache<String, String> cache0 = irc.getCache();
         String cacheName = cache0.getName();
         cache0.put("key", "value");
         AssertJUnit.assertEquals("value", cache0.get("key"));
         assertBasicIntelligence(irc, cacheName);

         irc.switchToCluster("backup");
         cache0.put("key1", "value1");
         AssertJUnit.assertEquals("value1", cache0.get("key1"));
         assertTopologyAwareIntelligence(irc, cacheName);

         irc.switchToDefaultCluster();
         cache0.put("key2", "value2");
         AssertJUnit.assertEquals("value2", cache0.get("key2"));
         assertBasicIntelligence(irc, cacheName);
      }
   }

   private RemoteCacheManager createClient(ClientIntelligence globalIntelligence, ClientIntelligence backupIntelligence) {
      ConfigurationBuilder builder = newRemoteConfigurationBuilder();
      if (globalIntelligence != null) {
         builder.clientIntelligence(globalIntelligence);
      }

      // first site
      addServer(builder.addServer(), siteName(0));

      // second site
      addServer(builder.addCluster("backup").clusterClientIntelligence(backupIntelligence), siteName(1));
      return new RemoteCacheManager(builder.build());
   }

   private HotRodServer getFirstServer(String siteName) {
      return siteServers.get(siteName).get(0);
   }

   private void addServer(ServerConfigurationBuilder builder, String siteName) {
      HotRodServer server = getFirstServer(siteName);
      builder.host(server.getHost()).port(server.getPort());
   }

   private void addServer(ClusterConfigurationBuilder builder, String siteName) {
      HotRodServer server = getFirstServer(siteName);
      builder.addClusterNode(server.getHost(), server.getPort());
   }

   private static void assertHashAwareIntelligence(RemoteCacheManager ircm, String cacheName) {
      OperationDispatcher dispatcher = Internals.dispatcher(ircm);

      log.debugf("Server list: %s", dispatcher.getServers(cacheName));
      log.debugf("Topology Info: %s", dispatcher.getCacheTopologyInfo(cacheName));
      log.debugf("Consistent Hash: %s", dispatcher.getConsistentHash(cacheName));

      AssertJUnit.assertEquals(2, dispatcher.getServers(cacheName).size());
      CacheTopologyInfo topologyInfo = dispatcher.getCacheTopologyInfo(cacheName);
      AssertJUnit.assertNotNull(topologyInfo);
      AssertJUnit.assertEquals(2, topologyInfo.getSegmentsPerServer().size());
      AssertJUnit.assertNotNull(topologyInfo.getNumSegments());
      AssertJUnit.assertNotNull(dispatcher.getConsistentHash(cacheName));
   }

   private static void assertTopologyAwareIntelligence(RemoteCacheManager ircm, String cacheName) {
      OperationDispatcher dispatcher = Internals.dispatcher(ircm);

      log.debugf("Server list: %s", dispatcher.getServers(cacheName));
      log.debugf("Topology Info: %s", dispatcher.getCacheTopologyInfo(cacheName));
      log.debugf("Consistent Hash: %s", dispatcher.getConsistentHash(cacheName));

      AssertJUnit.assertEquals(2, dispatcher.getServers(cacheName).size());
      CacheTopologyInfo topologyInfo = dispatcher.getCacheTopologyInfo(cacheName);
      AssertJUnit.assertNotNull(topologyInfo);
      AssertJUnit.assertEquals(2, topologyInfo.getSegmentsPerServer().size());
      AssertJUnit.assertNull(topologyInfo.getNumSegments());
      AssertJUnit.assertNull(dispatcher.getConsistentHash(cacheName));
   }

   private static void assertBasicIntelligence(RemoteCacheManager ircm, String cacheName) {
      OperationDispatcher dispatcher = Internals.dispatcher(ircm);

      log.debugf("Server list: %s", dispatcher.getServers(cacheName));
      log.debugf("Topology Info: %s", dispatcher.getCacheTopologyInfo(cacheName));
      log.debugf("Consistent Hash: %s", dispatcher.getConsistentHash(cacheName));

      AssertJUnit.assertEquals(1, dispatcher.getServers(cacheName).size());
      CacheTopologyInfo topologyInfo = dispatcher.getCacheTopologyInfo(cacheName);
      AssertJUnit.assertNotNull(topologyInfo);
      AssertJUnit.assertEquals(1, topologyInfo.getSegmentsPerServer().size());
      AssertJUnit.assertNull(topologyInfo.getNumSegments());
      AssertJUnit.assertNull(dispatcher.getConsistentHash(cacheName));
   }
}
