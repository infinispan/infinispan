package org.infinispan.client.hotrod.xsite;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.HitsAwareCacheManagersTest.HitCountInterceptor;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.xsite.AbstractXSiteTest;
import org.testng.annotations.AfterClass;

abstract class AbstractHotRodSiteFailoverTest extends AbstractXSiteTest {

   static String SITE_A = "LON";
   static String SITE_B = "NYC";
   static int NODES_PER_SITE = 2;

   Map<String, List<HotRodServer>> siteServers = new HashMap<>();

   RemoteCacheManager client(String siteName, Optional<String> backupSiteName) {
      HotRodServer server = siteServers.get(siteName).get(0);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      /*
       * Use 127.0.0.1 as host address to avoid the first PING after a cluster switch causing an invalidation of the
       * channel pools just to switch from localhost -> 127.0.0.1. This causes immediately subsequent ops to be
       * executed twice.
       */
      clientBuilder
         .addServer().host("127.0.0.1").port(server.getPort())
         .maxRetries(3); // Some retries so that shutdown nodes can be skipped
      clientBuilder.statistics().jmxEnable().jmxName(backupSiteName.orElse("default")).mBeanServerLookup(new PerThreadMBeanServerLookup());
      clientBuilder.asyncExecutorFactory().addExecutorProperty(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_THREADNAME_PREFIX, TestResourceTracker.getCurrentTestShortName());

      Optional<Integer> backupPort = backupSiteName.map(name -> {
         HotRodServer backupServer = siteServers.get(name).get(0);
         clientBuilder.addCluster(name).addClusterNode("127.0.0.1", backupServer.getPort());
         return backupServer.getPort();
      });

      if (backupPort.isPresent())
         log.debugf("Client for site '%s' connecting to main server in port %d, and backup cluster node port is %d",
            siteName, server.getPort(), backupPort.get());
      else
         log.debugf("Client for site '%s' connecting to main server in port %d",
            siteName, server.getPort());

      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   int findServerPort(String siteName) {
      return siteServers.get(siteName).get(0).getPort();
   }

   void killSite(String siteName) {
      log.debugf("Kill site '%s' with ports: %s", siteName,
         siteServers.get(siteName).stream().map(s -> String.valueOf(s.getPort())).collect(Collectors.joining(", ")));

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
         siteServers.values().forEach(servers ->
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
      globalBuilder.site().localSite(siteName);
      TestSite site = createSite(siteName, NODES_PER_SITE, globalBuilder, builder);
      Collection<EmbeddedCacheManager> cacheManagers = site.cacheManagers();
      List<HotRodServer> servers = cacheManagers.stream().map(cm -> serverPort
         .map(port -> HotRodClientTestingUtil.startHotRodServer(cm, port, new HotRodServerConfigurationBuilder().name(cm.getCacheManagerConfiguration().transport().nodeName())))
         .orElseGet(() -> HotRodClientTestingUtil.startHotRodServer(cm, new HotRodServerConfigurationBuilder().name(cm.getCacheManagerConfiguration().transport().nodeName())))).collect(Collectors.toList());
      siteServers.put(siteName, servers);

      log.debugf("Create site '%s' with ports: %s", siteName,
         servers.stream().map(s -> String.valueOf(s.getPort())).collect(Collectors.joining(", ")));
   }

   protected void addHitCountInterceptors() {
      siteServers.forEach((name, servers) ->
         servers.forEach(server -> {
            HitCountInterceptor interceptor = new HitCountInterceptor();
            server.getCacheManager().getCache().getAdvancedCache().getAsyncInterceptorChain()
                  .addInterceptor(interceptor, 1);
         })
      );
   }

   protected void assertNoHits() {
      siteServers.forEach((name, servers) ->
         servers.forEach(server -> {
            Cache<?, ?> cache = server.getCacheManager().getCache();
            HitCountInterceptor interceptor = getHitCountInterceptor(cache);
            assertEquals(0, interceptor.getHits());
         })
      );
   }

   protected void resetHitCounters() {
      siteServers.forEach((name, servers) ->
         servers.forEach(server -> {
            Cache<?, ?> cache = server.getCacheManager().getCache();
            HitCountInterceptor interceptor = getHitCountInterceptor(cache);
            interceptor.reset();
         })
      );
   }

   protected void assertSiteHit(String siteName, int expectedHits) {
      Stream<HotRodServer> serversHit = siteServers.get(siteName).stream().filter(server -> {
         Cache<?, ?> cache = server.getCacheManager().getCache();
         HitCountInterceptor interceptor = getHitCountInterceptor(cache);
         return interceptor.getHits() == expectedHits;
      });
      assertEquals(1, serversHit.count());
   }

   protected void assertSiteNotHit(String siteName) {
      siteServers.get(siteName).forEach(server -> {
         Cache<?, ?> cache = server.getCacheManager().getCache();
         HitCountInterceptor interceptor = getHitCountInterceptor(cache);
         assertEquals(0, interceptor.getHits());
      });
   }

   protected HitCountInterceptor getHitCountInterceptor(Cache<?, ?> cache) {
      return cache.getAdvancedCache().getAsyncInterceptorChain().findInterceptorWithClass(HitCountInterceptor.class);
   }

}
