package org.infinispan.it.compatibility;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.core.test.ServerTestingUtil.startProtocolServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.infinispan.test.TestingUtil.killCacheManagers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.apache.commons.httpclient.HttpClient;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
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
 * Compatibility cache factory taking care of construction and destruction of
 * caches, servers and clients for each of the endpoints being tested.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityCacheFactory<K, V> {

   private static final int DEFAULT_NUM_OWNERS = 2;

   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotrod;
   private RemoteCacheManager hotrodClient;
   private RestServer rest;
   private MemcachedServer memcached;

   private Cache<K, V> embeddedCache;
   private RemoteCache<K, V> hotrodCache;
   private HttpClient restClient;
   private MemcachedClient memcachedClient;
   private Transcoder transcoder;

   private final String cacheName;
   private final Marshaller marshaller;
   private final CacheMode cacheMode;
   private final int numOwners;
   private final boolean l1Enable;
   private final boolean memcachedWithDecoder;
   private int restPort;

   CompatibilityCacheFactory(CacheMode cacheMode) {
      this(cacheMode, DEFAULT_NUM_OWNERS, false);
   }

   CompatibilityCacheFactory(CacheMode cacheMode, int numOwners, boolean l1Enable) {
      this("", null, cacheMode, numOwners, l1Enable, null, null);
   }

   CompatibilityCacheFactory(CacheMode cacheMode, int numOwners, boolean l1Enable, Encoder encoder) {
      this("", null, cacheMode, numOwners, l1Enable, null, encoder);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode) {
      this(cacheName, marshaller, cacheMode, DEFAULT_NUM_OWNERS, null);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, Encoder encoder) {
      this(cacheName, marshaller, cacheMode, DEFAULT_NUM_OWNERS, false, null, encoder);
   }


   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners, Encoder encoder) {
      this(cacheName, marshaller, cacheMode, numOwners, false, null, encoder);
   }

   public CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, Transcoder transcoder) {
      this(cacheName, marshaller, cacheMode, DEFAULT_NUM_OWNERS, false, transcoder, null);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners, boolean l1Enable,
                             Transcoder transcoder, Encoder encoder) {
      this.cacheName = cacheName;
      this.marshaller = marshaller;
      this.cacheMode = cacheMode;
      this.numOwners = numOwners;
      this.l1Enable = l1Enable;
      this.transcoder = transcoder;
      this.memcachedWithDecoder = transcoder != null;
   }

   public CompatibilityCacheFactory<K, V> setup() throws Exception {
      createEmbeddedCache();
      createHotRodCache();
      createRestMemcachedCaches();
      return this;
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

      org.infinispan.configuration.cache.ConfigurationBuilder builder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode)
            .compatibility().enable().marshaller(marshaller);

      if (cacheMode.isDistributed() && numOwners != DEFAULT_NUM_OWNERS) {
         builder.clustering().hash().numOwners(numOwners);
      }

      if (cacheMode.isDistributed() && l1Enable) {
         builder.clustering().l1().enable();
      }

      cacheManager = cacheMode.isClustered()
            ? TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder)
            : TestCacheManagerFactory.createCacheManager(globalBuilder, builder);

      if (!cacheName.isEmpty())
         cacheManager.defineConfiguration(cacheName, builder.build());

      embeddedCache = cacheName.isEmpty()
            ? cacheManager.getCache()
            : cacheManager.getCache(cacheName);
   }

   private void createHotRodCache() {
      createHotRodCache(startHotRodServer(cacheManager));
   }

   private void createHotRodCache(HotRodServer server) {
      hotrod = server;
      hotrodClient = new RemoteCacheManager(new ConfigurationBuilder()
            .addServers("localhost:" + hotrod.getPort())
            .addJavaSerialWhiteList(".*Person.*", ".*CustomEvent.*")
            .marshaller(marshaller)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.getCache()
            : hotrodClient.getCache(cacheName);
   }

   private void createRestCache() throws Exception {
      RestServer restServer = startProtocolServer(findFreePort(), p -> {
         RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
         builder.port(p);
         rest = new RestServer();
         rest.start(builder.build(), cacheManager);
         return rest;
      });
      restPort = restServer.getPort();
      restClient = new HttpClient();
   }

   private void createMemcachedCache() throws IOException {
      memcached = startProtocolServer(findFreePort(), p -> {
         if (memcachedWithDecoder) {
            return startMemcachedTextServer(cacheManager, p, cacheName);
         }
         return startMemcachedTextServer(cacheManager, p);
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

   public static void killCacheFactories(CompatibilityCacheFactory... cacheFactories) {
      if (cacheFactories != null) {
         for (CompatibilityCacheFactory cacheFactory : cacheFactories) {
            if (cacheFactory != null)
               cacheFactory.teardown();
         }
      }
   }

   void teardown() {
      killRemoteCacheManager(hotrodClient);
      killServers(hotrod);
      killRestServer(rest);
      killMemcachedClient(memcachedClient);
      killMemcachedServer(memcached);
      killCacheManagers(cacheManager);
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

   public HttpClient getRestClient() {
      return restClient;
   }

   public MemcachedClient getMemcachedClient() {
      return memcachedClient;
   }

   int getMemcachedPort() {
      return memcached.getPort();
   }

   public String getRestUrl() {
      String restCacheName = cacheName.isEmpty() ? BasicCacheContainer.DEFAULT_CACHE_NAME : cacheName;
      return String.format("http://localhost:%s/rest/%s", restPort, restCacheName);
   }

   HotRodServer getHotrodServer() {
      return hotrod;
   }

   public void registerEncoder(Encoder encoder) {
      EncoderRegistry encoderRegistry = embeddedCache.getAdvancedCache().getComponentRegistry()
            .getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
      encoderRegistry.registerEncoder(encoder);
   }
}
