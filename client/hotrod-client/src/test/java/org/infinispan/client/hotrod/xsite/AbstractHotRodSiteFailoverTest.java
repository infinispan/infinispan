package org.infinispan.client.hotrod.xsite;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractXSiteTest;
import org.testng.annotations.AfterClass;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

abstract class AbstractHotRodSiteFailoverTest extends AbstractXSiteTest {

   static String SITE_A = "LON";
   static String SITE_B = "NYC";
   static Random R = new Random();
   static int NODES_PER_SITE = 2;

   Map<String, List<HotRodServer>> siteServers = new HashMap<>();

   RemoteCacheManager client(String siteName, Optional<String> backupSiteName) {
      HotRodServer server = siteServers.get(siteName).get(R.nextInt(NODES_PER_SITE));
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
         .addServer().host("localhost").port(server.getPort())
         .maxRetries(0);

      backupSiteName.ifPresent(name -> {
         HotRodServer backupServer = siteServers.get(name).get(R.nextInt(NODES_PER_SITE));
         clientBuilder.addCluster(name).addClusterNode("localhost", backupServer.getPort());
      });
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   int findServerPort(String siteName) {
      return siteServers.get(siteName).get(0).getPort();
   }

   void killSite(String siteName) {
      siteServers.get(siteName).forEach(HotRodClientTestingUtil::killServers);
      site(siteName).cacheManagers().forEach(TestingUtil::killCacheManagers);
   }

   @Override
   protected void createSites() {
      createHotRodSite(SITE_A, SITE_B, Optional.empty());
      createHotRodSite(SITE_B, SITE_A, Optional.empty());
   }

   @AfterClass(alwaysRun = true) // run even if the test failed
   protected void destroy() {
      try {
         siteServers.values().stream().forEach(servers ->
            servers.forEach(HotRodClientTestingUtil::killServers));
      } finally {
         super.destroy();
      }
   }

   protected void createHotRodSite(String siteName, String backupSiteName, Optional<Integer> serverPort) {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      BackupConfigurationBuilder backup = builder.sites().addBackup();
      backup.site(backupSiteName).strategy(BackupStrategy.SYNC);

      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.globalJmxStatistics().allowDuplicateDomains(true);
      globalBuilder.site().localSite(siteName);
      TestSite site = createSite(siteName, NODES_PER_SITE, globalBuilder, builder);
      Collection<EmbeddedCacheManager> cacheManagers = site.cacheManagers();
      List<HotRodServer> servers = cacheManagers.stream().map(cm -> serverPort
         .map(port -> HotRodClientTestingUtil.startHotRodServer(cm, port, new HotRodServerConfigurationBuilder()))
         .orElseGet(() -> HotRodClientTestingUtil.startHotRodServer(cm))).collect(Collectors.toList());
      siteServers.put(siteName, servers);
   }

}
