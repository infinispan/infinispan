package org.infinispan.it.endpoints;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.core.test.ServerTestingUtil.startProtocolServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.infinispan.test.TestingUtil.killCacheManagers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Takes care of construction and destruction of caches, servers and clients for each of the endpoints being tested.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class EndpointsCacheFactory<K, V> {

   private static final int DEFAULT_NUM_OWNERS = 2;

   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotrod;
   private RemoteCacheManager hotrodClient;
   private RestServer rest;
   private MemcachedServer memcached;

   private Cache<K, V> embeddedCache;
   private RemoteCache<K, V> hotrodCache;
   private RestClient restClient;
   private RestCacheClient restCacheClient;
   private MemcachedClient memcachedClient;
   private final Transcoder<Object> transcoder;

   private final String cacheName;
   private final Marshaller marshaller;
   private final CacheMode cacheMode;
   private final SerializationContextInitializer contextInitializer;
   private final int numOwners;
   private final boolean l1Enable;
   private final boolean memcachedWithDecoder;

   private EndpointsCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners, boolean l1Enable,
                                 Transcoder<Object> transcoder, SerializationContextInitializer contextInitializer) {
      this.cacheName = cacheName;
      this.marshaller = marshaller;
      this.cacheMode = cacheMode;
      this.numOwners = numOwners;
      this.l1Enable = l1Enable;
      this.transcoder = transcoder;
      this.memcachedWithDecoder = transcoder != null;
      this.contextInitializer = contextInitializer;
   }

   private EndpointsCacheFactory<K, V> setup() throws Exception {
      createEmbeddedCache();
      createHotRodCache();
      createRestMemcachedCaches();
      return this;
   }

   void addRegexAllowList(String regex) {
      cacheManager.getClassAllowList().addRegexps(regex);
   }

   private void createRestMemcachedCaches() throws Exception {
      createRestCache();
      createMemcachedCache();
   }

   private void createEmbeddedCache() {
      GlobalConfigurationBuilder globalBuilder;

      if (cacheMode.isClustered()) {
         globalBuilder = new GlobalConfigurationBuilder();
         globalBuilder.transport().defaultTransport();
      } else {
         globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      }
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      globalBuilder.defaultCacheName(cacheName);
      if (contextInitializer != null)
         globalBuilder.serialization().addContextInitializer(contextInitializer);

      org.infinispan.configuration.cache.ConfigurationBuilder builder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode)
            .encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      if (cacheMode.isDistributed() && numOwners != DEFAULT_NUM_OWNERS) {
         builder.clustering().hash().numOwners(numOwners);
      }

      if (cacheMode.isDistributed() && l1Enable) {
         builder.clustering().l1().enable();
      }

      cacheManager = cacheMode.isClustered()
            ? TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder)
            : TestCacheManagerFactory.createCacheManager(globalBuilder, builder);

      embeddedCache = cacheManager.getCache(cacheName);

      EncoderRegistry encoderRegistry = embeddedCache.getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry().getComponent(EncoderRegistry.class);

      if (marshaller != null) {
         boolean isConversionSupported = encoderRegistry.isConversionSupported(marshaller.mediaType(), APPLICATION_OBJECT);
         if (!isConversionSupported) {
            encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(marshaller));
         }
      }
   }

   private void createHotRodCache() {
      createHotRodCache(startHotRodServer(cacheManager));
   }

   private void createHotRodCache(HotRodServer server) {
      hotrod = server;
      hotrodClient = new RemoteCacheManager(new ConfigurationBuilder()
            .addServers("localhost:" + hotrod.getPort())
            .addJavaSerialAllowList(".*Person.*", ".*CustomEvent.*")
            .marshaller(marshaller)
            .addContextInitializer(contextInitializer)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.getCache()
            : hotrodClient.getCache(cacheName);
   }

   private void createRestCache() {
      RestServer restServer = startProtocolServer(findFreePort(), p -> {
         RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
         builder.port(p);
         rest = new RestServer();
         rest.start(builder.build(), cacheManager);
         return rest;
      });
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort());
      restClient = RestClient.forConfiguration(builder.build());
      restCacheClient = restClient.cache(cacheName);
   }

   private void createMemcachedCache() throws IOException {
      MediaType clientEncoding = marshaller == null ? MediaType.APPLICATION_OCTET_STREAM : marshaller.mediaType();
      memcached = startProtocolServer(findFreePort(), p -> {
         if (memcachedWithDecoder) {
            return startMemcachedTextServer(cacheManager, p, cacheName, clientEncoding);
         }
         return startMemcachedTextServer(cacheManager, p, clientEncoding);
      });
      memcachedClient = createMemcachedClient(60000, memcached.getPort());
   }

   private MemcachedClient createMemcachedClient(long timeout, int port) throws IOException {
      ConnectionFactory cf = new DefaultConnectionFactory() {
         @Override
         public long getOperationTimeout() {
            return timeout;
         }
      };

      if (transcoder != null) {
         cf = new ConnectionFactoryBuilder(cf).setTranscoder(transcoder).build();
      }
      return new MemcachedClient(cf, Collections.singletonList(new InetSocketAddress("127.0.0.1", port)));
   }

   public static void killCacheFactories(EndpointsCacheFactory... cacheFactories) {
      if (cacheFactories != null) {
         for (EndpointsCacheFactory cacheFactory : cacheFactories) {
            if (cacheFactory != null)
               cacheFactory.teardown();
         }
      }
   }

   void teardown() {
      Util.close(restClient);
      restClient = null;
      killRemoteCacheManager(hotrodClient);
      hotrodClient = null;
      killServers(hotrod);
      hotrod = null;
      killRestServer(rest);
      rest = null;
      killMemcachedClient(memcachedClient);
      memcachedClient = null;
      killMemcachedServer(memcached);
      memcached = null;
      killCacheManagers(cacheManager);
      cacheManager = null;
   }

   private void killRestServer(RestServer rest) {
      if (rest != null) {
         try {
            rest.stop();
         } catch (Exception e) {
            // Ignore
         }
      }
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }

   public Cache<K, V> getEmbeddedCache() {
      return (Cache<K, V>) embeddedCache.getAdvancedCache().withEncoding(IdentityEncoder.class);
   }

   public RemoteCache<K, V> getHotRodCache() {
      return hotrodCache;
   }

   public MemcachedClient getMemcachedClient() {
      return memcachedClient;
   }

   int getMemcachedPort() {
      return memcached.getPort();
   }

   public RestCacheClient getRestCacheClient() {
      return restCacheClient;
   }

   HotRodServer getHotrodServer() {
      return hotrod;
   }

   public static class Builder<K, V> {
      private CacheMode cacheMode;
      private int numOwners = DEFAULT_NUM_OWNERS;
      private boolean l1Enable = false;
      private SerializationContextInitializer contextInitializer = null;
      private String cacheName = "test";
      private Marshaller marshaller = null;
      private Transcoder<Object> transcoder = null;

      public Builder<K, V> withCacheMode(CacheMode cacheMode) {
         this.cacheMode = cacheMode;
         return this;
      }

      public Builder<K, V> withNumOwners(int numOwners) {
         this.numOwners = numOwners;
         return this;
      }

      public Builder<K, V> withL1(boolean l1Enable) {
         this.l1Enable = l1Enable;
         return this;
      }

      public Builder<K, V> withContextInitializer(SerializationContextInitializer contextInitializer) {
         this.contextInitializer = contextInitializer;
         return this;
      }

      public Builder<K, V> withCacheName(String cacheName) {
         this.cacheName = cacheName;
         return this;
      }

      public Builder<K, V> withMarshaller(Marshaller marshaller) {
         this.marshaller = marshaller;
         return this;
      }

      public Builder<K, V> withMemcachedTranscoder(Transcoder<Object> transcoder) {
         this.transcoder = transcoder;
         return this;
      }

      public EndpointsCacheFactory<K, V> build() throws Exception {
         EndpointsCacheFactory<K, V> endpointsCacheFactory =
               new EndpointsCacheFactory<>(cacheName, marshaller, cacheMode, numOwners, l1Enable, transcoder, contextInitializer);
         return endpointsCacheFactory.setup();
      }
   }

}
