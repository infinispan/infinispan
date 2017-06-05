package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.client.hotrod.ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.upgrade.RollingUpgradeManager;


class TestCluster {

   private List<HotRodServer> hotRodServers;
   private List<EmbeddedCacheManager> embeddedCacheManagers;
   private RemoteCacheManager remoteCacheManager;

   private TestCluster(List<HotRodServer> hotRodServers, List<EmbeddedCacheManager> embeddedCacheManagers,
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

   Cache<Object, Object> getEmbeddedCache(String name) {
      return embeddedCacheManagers.get(0).getCache(name);
   }

   List<Cache<String, String>> getEmbeddedCaches(String name) {
      return embeddedCacheManagers.stream().map(cm -> cm.<String, String>getCache(name)).collect(Collectors.toList());
   }


   RollingUpgradeManager getRollingUpgradeManager(String cacheName) {
      return embeddedCacheManagers.get(0).getCache(cacheName).getAdvancedCache().getComponentRegistry()
            .getComponent(RollingUpgradeManager.class);
   }

   int getHotRodPort() {
      return hotRodServers.get(0).getPort();
   }

   void cleanAllCaches() {
      embeddedCacheManagers.stream().flatMap(m -> m.getCacheNames().stream().map(m::getCache)).forEach(Cache::clear);
   }

   static class Builder {
      private String name = "cluster1";
      private int numMembers = 1;
      private Map<String, ConfigurationBuilder> caches = new HashMap<>();
      private HotRodServerConfigurationBuilder hotRodBuilder = new HotRodServerConfigurationBuilder();
      private String trustStoreFileName;
      private char[] trustStorePassword;
      private char[] keyStorePassword;
      private String keyStoreFileName;

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
         private String protocolVersion = DEFAULT_PROTOCOL_VERSION.toString();
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

         TestCluster build() {
            addNewCache();
            return builder.build();
         }

         private CacheDefinitionBuilder addNewCache() {
            if (configurationBuilder == null)
               configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
            if (remotePort != null) {
               RemoteStoreConfigurationBuilder store = configurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
               store.hotRodWrapping(true)
                     .remoteCacheName(name).protocolVersion(protocolVersion).shared(true)
                     .addServer().host("localhost").port(remotePort);
               if (builder.trustStoreFileName != null) {
                  store.remoteSecurity().ssl().enable().trustStoreFileName(builder.trustStoreFileName).trustStorePassword(builder.trustStorePassword);
               }
               if (builder.keyStoreFileName != null) {
                  store.remoteSecurity().ssl().keyStoreFileName(builder.keyStoreFileName).keyStorePassword(builder.keyStorePassword);
               }
            }
            builder.addCache(name, configurationBuilder);
            return new CacheDefinitionBuilder(builder);
         }
      }

      Builder withHotRodBuilder(HotRodServerConfigurationBuilder hotRodBuilder) {
         this.hotRodBuilder = hotRodBuilder;
         return this;
      }

      Builder withSSLTrustStore(String trustStoreFileName, char[] trustStorePassword) {
         this.trustStoreFileName = trustStoreFileName;
         this.trustStorePassword = trustStorePassword;
         return this;
      }

      Builder withSSLKeyStore(String keyStoreFileName, char[] keyStorePassword) {
         this.keyStoreFileName = keyStoreFileName;
         this.keyStorePassword = keyStorePassword;
         return this;
      }


      private void addCache(String name, ConfigurationBuilder cfg) {
         caches.put(name, cfg);
      }

      public TestCluster build() {
         List<HotRodServer> hotRodServers = new ArrayList<>();
         List<EmbeddedCacheManager> embeddedCacheManagers = new ArrayList<>();

         for (int i = 0; i < numMembers; i++) {
            GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
            gcb.transport().defaultTransport().clusterName(name);
            EmbeddedCacheManager clusteredCacheManager =
                  createClusteredCacheManager(gcb, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
            caches.entrySet().forEach(entry ->
                  clusteredCacheManager.defineConfiguration(entry.getKey(), entry.getValue().build()));

            embeddedCacheManagers.add(clusteredCacheManager);
            hotRodServers.add(HotRodClientTestingUtil.startHotRodServer(clusteredCacheManager, hotRodBuilder));
         }

         int port = hotRodServers.get(0).getPort();
         org.infinispan.client.hotrod.configuration.ConfigurationBuilder build = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         build.addServer().port(port).host("localhost");
         if (trustStoreFileName != null) {
            build.security().ssl().enable().trustStoreFileName(trustStoreFileName).trustStorePassword(trustStorePassword);
         }
         if (keyStoreFileName != null) {
            build.security().ssl().keyStoreFileName(keyStoreFileName).keyStorePassword(keyStorePassword);
         }

         return new TestCluster(hotRodServers, embeddedCacheManagers, new RemoteCacheManager(build.build()));
      }

   }
}
