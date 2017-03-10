package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.upgrade.RollingUpgradeManager;


public class Cluster {

   private List<HotRodServer> hotRodServers;
   private List<EmbeddedCacheManager> embeddedCacheManagers;
   private RemoteCacheManager remoteCacheManager;

   private Cluster(List<HotRodServer> hotRodServers, List<EmbeddedCacheManager> embeddedCacheManagers,
                   RemoteCacheManager remoteCacheManager) {
      this.hotRodServers = hotRodServers;
      this.embeddedCacheManagers = embeddedCacheManagers;
      this.remoteCacheManager = remoteCacheManager;
   }

   RemoteCache<String, String> getRemoteCache(String cacheName) {
      return remoteCacheManager.getCache(cacheName);
   }

   void destroy() {
      embeddedCacheManagers.forEach(TestingUtil::killCacheManagers);
      hotRodServers.forEach(HotRodClientTestingUtil::killServers);
      HotRodClientTestingUtil.killRemoteCacheManagers(remoteCacheManager);
   }

   Cache<String, String> getEmbeddedCache(String name) {
      return embeddedCacheManagers.get(0).getCache(name);
   }


   RollingUpgradeManager getRollingUpgradeManager(String cacheName) {
      return embeddedCacheManagers.get(0).getCache(cacheName).getAdvancedCache().getComponentRegistry()
            .getComponent(RollingUpgradeManager.class);
   }

   int getHotRodPort() {
      return hotRodServers.get(0).getAddress().port();
   }

   void cleanAllCaches() {
      embeddedCacheManagers.stream().flatMap(m -> m.getCacheNames().stream().map(m::getCache)).forEach(Cache::clear);
   }

   static class Builder {
      private String name = "cluster1";
      private int numMembers = 1;
      private Map<String, ConfigurationBuilder> caches = new HashMap<>();

      Builder setNumMembers(int numMembers) {
         this.numMembers = numMembers;
         return this;
      }

      public Builder setName(String name) {
         this.name = name;
         return this;
      }

      CacheDefinitionBuilder cache() {
         return new CacheDefinitionBuilder(this);
      }

      static class CacheDefinitionBuilder {
         private final Builder builder;
         private ConfigurationBuilder configurationBuilder;
         private String name;
         private String protocolVersion = ConfigurationProperties.DEFAULT_PROTOCOL_VERSION;
         private Integer remotePort;

         CacheDefinitionBuilder(Builder builder) {
            this.builder = builder;
         }

         public CacheDefinitionBuilder name(String name) {
            this.name = name;
            return this;
         }

         CacheDefinitionBuilder remotePort(Integer remotePort) {
            this.remotePort = remotePort;
            return this;
         }

         CacheDefinitionBuilder remoteProtocolVersion(String remoteVersion) {
            this.protocolVersion = remoteVersion;
            return this;
         }

         CacheDefinitionBuilder configuredWith(ConfigurationBuilder configurationBuilder) {
            this.configurationBuilder = configurationBuilder;
            return this;
         }

         CacheDefinitionBuilder cache() {
            return addNewCache();
         }

         Cluster build() {
            addNewCache();
            return builder.build();
         }

         private CacheDefinitionBuilder addNewCache() {
            if (configurationBuilder == null)
               configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
            if (remotePort != null) {
               configurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class).hotRodWrapping(true)
                     .remoteCacheName(name).ignoreModifications(true).protocolVersion(protocolVersion)
                     .addServer().host("localhost").port(remotePort);
            }
            builder.addCache(name, configurationBuilder);
            return new CacheDefinitionBuilder(builder);
         }

      }

      private void addCache(String name, ConfigurationBuilder cfg) {
         caches.put(name, cfg);
      }

      public Cluster build() {
         List<HotRodServer> hotRodServers = new ArrayList<>();
         List<EmbeddedCacheManager> embeddedCacheManagers = new ArrayList<>();

         for (int i = 0; i < numMembers; i++) {
            GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
            gcb.transport().defaultTransport().clusterName(name);
            EmbeddedCacheManager clusteredCacheManager =
                  createClusteredCacheManager(gcb, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
            caches.entrySet().forEach(entry -> {
               ConfigurationBuilder builder = entry.getValue();
               builder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());
               clusteredCacheManager.defineConfiguration(entry.getKey(), hotRodCacheConfiguration(builder).build());
            });

            embeddedCacheManagers.add(clusteredCacheManager);
            hotRodServers.add(HotRodClientTestingUtil.startHotRodServer(clusteredCacheManager));
         }

         int port = hotRodServers.get(0).getAddress().port();
         Configuration build = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
               .addServer().port(port).host("localhost").build();

         return new Cluster(hotRodServers, embeddedCacheManagers, new RemoteCacheManager(build));
      }

   }
}
