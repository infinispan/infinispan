package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.client.hotrod.ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
import static org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_23;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;
import static org.infinispan.test.TestingUtil.waitForNoRebalanceAcrossManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.ServerAddress;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.testng.Assert;


class TestCluster {

   private List<HotRodServer> hotRodServers;
   private List<EmbeddedCacheManager> embeddedCacheManagers;
   private RemoteCacheManager remoteCacheManager;
   private EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();

   private TestCluster(List<HotRodServer> hotRodServers, List<EmbeddedCacheManager> embeddedCacheManagers,
                       RemoteCacheManager remoteCacheManager) {
      this.hotRodServers = hotRodServers;
      this.embeddedCacheManagers = embeddedCacheManagers;
      this.remoteCacheManager = remoteCacheManager;
   }

   <K, V> RemoteCache<K, V> getRemoteCache(String cacheName) {
      return remoteCacheManager.getCache(cacheName);
   }

   <K, V> RemoteCache<K, V> getRemoteCache(String cacheName, boolean transactional) {
      if (!transactional) {
         return getRemoteCache(cacheName);
      } else {
         return remoteCacheManager.getCache(cacheName, TransactionMode.NON_XA, transactionManager);
      }
   }

   void destroy() {
      embeddedCacheManagers.forEach(TestingUtil::killCacheManagers);
      embeddedCacheManagers.clear();
      hotRodServers.forEach(HotRodClientTestingUtil::killServers);
      hotRodServers.clear();
      HotRodClientTestingUtil.killRemoteCacheManagers(remoteCacheManager);
      remoteCacheManager = null;
      EmbeddedTransactionManager.destroy();
   }

   <K, V> Cache<K, V> getEmbeddedCache(String name) {
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

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      String members = embeddedCacheManagers.stream().map(EmbeddedCacheManager::getMembers).map(Object::toString).collect(Collectors.joining(","));
      sb.append("Cluster members: ").append(members);
      String addresses = hotRodServers.stream().map(HotRodServer::getAddress).map(ServerAddress::toString).collect(Collectors.joining(","));
      sb.append("Servers: ").append(addresses);
      sb.append(addresses);
      return sb.toString();
   }

   public void disconnectSource(String cacheName) {
      embeddedCacheManagers.forEach(c -> {
         RollingUpgradeManager rum = c.getCache(cacheName).getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
         try {
            rum.disconnectSource("hotrod");
         } catch (Exception e) {
            Assert.fail("Failed to disconnect source!");
         }
      });
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
      private boolean segmented;
      private SerializationCtx ctx;
      private Class<? extends Marshaller> marshaller;

      Builder setNumMembers(int numMembers) {
         this.numMembers = numMembers;
         return this;
      }

      public Builder marshaller(Class<? extends Marshaller> marshaller) {
         this.marshaller = marshaller;
         return this;
      }

      public Builder ctx(SerializationCtx ctx) {
         this.ctx = ctx;
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
         private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;
         private Integer remotePort;
         private boolean wrapping = true;
         private boolean rawValues = true;
         private Class<? extends Marshaller> marshaller;

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

         CacheDefinitionBuilder remoteStoreWrapping(boolean wrapping) {
            this.wrapping = wrapping;
            return this;
         }

         CacheDefinitionBuilder remoteStoreRawValues(boolean rawValues) {
            this.rawValues = rawValues;
            return this;
         }

         CacheDefinitionBuilder remoteStoreMarshaller(Class<? extends Marshaller> marshaller) {
            this.marshaller = marshaller;
            return this;
         }

         CacheDefinitionBuilder remoteProtocolVersion(ProtocolVersion version) {
            this.protocolVersion = version;
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
               store.hotRodWrapping(wrapping).rawValues(rawValues)
                     .remoteCacheName(name).protocolVersion(protocolVersion).shared(true)
                     .addServer().host("localhost").port(remotePort);
               if (builder.trustStoreFileName != null) {
                  store.remoteSecurity().ssl().enable().trustStoreFileName(builder.trustStoreFileName).trustStorePassword(builder.trustStorePassword);
               }
               if (builder.keyStoreFileName != null) {
                  store.remoteSecurity().ssl().keyStoreFileName(builder.keyStoreFileName).keyStorePassword(builder.keyStorePassword);
               }
               if (builder.segmented) {
                  if (protocolVersion.compareTo(PROTOCOL_VERSION_23) < 0) {
                     throw new AssertionError("Protocol version less than 2.3 cannot be segmented");
                  }
                  store.segmented(true);
               } else {
                  store.segmented(false);
               }
               if(marshaller != null) {
                  store.marshaller(marshaller);
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

      Builder segmented(boolean segmented) {
         this.segmented = segmented;
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
            gcb.serialization().allowList().addClasses(CustomObject.class);
            if (ctx != null) {
               gcb.serialization().addContextInitializer(ctx);
            }
            gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
            gcb.transport().defaultTransport().clusterName(name);
            EmbeddedCacheManager clusteredCacheManager =
                  createClusteredCacheManager(gcb, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
            caches.forEach((key, value) -> clusteredCacheManager.defineConfiguration(key, value.build()));
            embeddedCacheManagers.add(clusteredCacheManager);
            hotRodServers.add(HotRodClientTestingUtil.startHotRodServer(clusteredCacheManager, hotRodBuilder));
         }

         embeddedCacheManagers.forEach(cm -> caches.keySet().forEach(cm::getCache));
         waitForNoRebalanceAcrossManagers(embeddedCacheManagers.toArray(new EmbeddedCacheManager[0]));

         int port = hotRodServers.get(0).getPort();
         org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         clientBuilder.addServer().port(port).host("localhost");
         if (ctx != null) {
            clientBuilder.addContextInitializer(ctx);
         }
         if (trustStoreFileName != null) {
            clientBuilder.security().ssl().enable().trustStoreFileName(trustStoreFileName).trustStorePassword(trustStorePassword);
         }
         if (keyStoreFileName != null) {
            clientBuilder.security().ssl().keyStoreFileName(keyStoreFileName).keyStorePassword(keyStorePassword);
         }
         if (marshaller != null) {
            clientBuilder.marshaller(marshaller);
         } else {
            clientBuilder.marshaller(GenericJBossMarshaller.class);
         }
         clientBuilder.addJavaSerialAllowList(".*");
         return new TestCluster(hotRodServers, embeddedCacheManagers, new RemoteCacheManager(clientBuilder.build()));
      }

   }
}
