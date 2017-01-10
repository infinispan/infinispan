package org.infinispan.it.compatibility;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
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
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.NettyRestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
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
   private Lifecycle rest;
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
   private Equivalence keyEquivalence = null;
   private Equivalence valueEquivalence = null;

   CompatibilityCacheFactory(CacheMode cacheMode) {
      this(cacheMode, DEFAULT_NUM_OWNERS, false);
   }

   CompatibilityCacheFactory(CacheMode cacheMode, int numOwners, boolean l1Enable) {
      this("", null, cacheMode, numOwners, l1Enable, null);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode) {
      this(cacheName, marshaller, cacheMode, DEFAULT_NUM_OWNERS);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners) {
      this(cacheName, marshaller, cacheMode, numOwners, false, null);
   }

   public CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, Transcoder transcoder) {
      this(cacheName, marshaller, cacheMode, DEFAULT_NUM_OWNERS, false, transcoder);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners, boolean l1Enable,
                             Transcoder transcoder) {
      this.cacheName = cacheName;
      this.marshaller = marshaller;
      this.cacheMode = cacheMode;
      this.numOwners = numOwners;
      this.l1Enable = l1Enable;
      this.transcoder = transcoder;
      this.memcachedWithDecoder = transcoder != null;
   }

   CompatibilityCacheFactory<K, V> keyEquivalence(Equivalence equivalence) {
      this.keyEquivalence = equivalence;
      return this;
   }

   CompatibilityCacheFactory<K, V> valueEquivalence(Equivalence equivalence) {
      this.valueEquivalence = equivalence;
      return this;
   }

   public CompatibilityCacheFactory<K, V> setup() throws Exception {
      createEmbeddedCache();
      createHotRodCache();
      createRestMemcachedCaches();
      return this;
   }

   CompatibilityCacheFactory<K, V> setup(int baseHotRodPort, int portOffset) throws Exception {
      createEmbeddedCache();
      createHotRodCache(baseHotRodPort + portOffset);
      createRestMemcachedCaches(portOffset);
      return this;
   }

   private void createRestMemcachedCaches() throws Exception {
      restPort = hotrod.getPort() + 20;
      final int memcachedPort = hotrod.getPort() + 40;
      createRestCache(restPort);
      createMemcachedCache(memcachedPort);
   }

   private void createRestMemcachedCaches(int portOffset) throws Exception {
      restPort = hotrod.getPort() + 20 + portOffset;
      final int memcachedPort = hotrod.getPort() + 40 + portOffset;
      createRestCache(restPort);
      createMemcachedCache(memcachedPort);
   }

   void createEmbeddedCache() {
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

      if (keyEquivalence != null) {
         builder.dataContainer().keyEquivalence(keyEquivalence);
      }

      if (valueEquivalence != null) {
         builder.dataContainer().valueEquivalence(valueEquivalence);
      }

      cacheManager = cacheMode.isClustered()
            ? TestCacheManagerFactory.createClusteredCacheManager(builder)
            : TestCacheManagerFactory.createCacheManager(builder);

      if (!cacheName.isEmpty())
         cacheManager.defineConfiguration(cacheName, builder.build());

      embeddedCache = cacheName.isEmpty()
            ? cacheManager.getCache()
            : cacheManager.getCache(cacheName);
   }

   private void createHotRodCache() {
      createHotRodCache(startHotRodServer(cacheManager));
   }

   private void createHotRodCache(int port) {
      createHotRodCache(HotRodTestingUtil
            .startHotRodServer(cacheManager, port));
   }

   private void createHotRodCache(HotRodServer server) {
      hotrod = server;
      hotrodClient = new RemoteCacheManager(new ConfigurationBuilder()
            .addServers("localhost:" + hotrod.getPort())
            .marshaller(marshaller)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.getCache()
            : hotrodClient.getCache(cacheName);
   }

   void createRestCache(int port) throws Exception {
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.port(port);
      rest = NettyRestServer.createServer(builder.build(), cacheManager);
      rest.start();
      restClient = new HttpClient();
   }

   private void createMemcachedCache(int port) throws IOException {
      memcached = memcachedWithDecoder ? startMemcachedTextServer(cacheManager, port, cacheName) : startMemcachedTextServer(cacheManager, port);
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

   void killRestServer(Lifecycle rest) {
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
      return embeddedCache;
   }

   public RemoteCache<K, V> getHotRodCache() {
      return hotrodCache;
   }

   int getHotRodPort() {
      return hotrod.getPort();
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

}
