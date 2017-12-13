package org.infinispan.it.compatibility;

import net.spy.memcached.MemcachedClient;
import org.apache.commons.httpclient.HttpClient;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.rest.NettyRestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;

/**
 * Compatibility cache factory taking care of construction and destruction of
 * caches, servers and clients for each of the endpoints being tested.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityCacheFactory<K, V> {

   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotrod;
   private RemoteCacheManager hotrodClient;
   private Lifecycle rest;
   private MemcachedServer memcached;

   private Cache<K, V> embeddedCache;
   private RemoteCache<K, V> hotrodCache;
   private HttpClient restClient;
   private MemcachedClient memcachedClient;

   private final String cacheName;
   private final Marshaller marshaller;
   private final CacheMode cacheMode;
   private int restPort;
   private final int defaultNumOwners = 2;
   private int numOwners = defaultNumOwners;
   private boolean l1Enable = false;
   private Equivalence keyEquivalence = null;
   private Equivalence valueEquivalence = null;

   CompatibilityCacheFactory(CacheMode cacheMode) {
      this.cacheName = "";
      this.marshaller = null;
      this.cacheMode = cacheMode;
   }

   CompatibilityCacheFactory(CacheMode cacheMode, int numOwners, boolean l1Enable) {
      this(cacheMode);
      this.numOwners = numOwners;
      this.l1Enable = l1Enable;
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode) {
      this.cacheName = cacheName;
      this.marshaller = marshaller;
      this.cacheMode = cacheMode;
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller, CacheMode cacheMode, int numOwners) {
      this(cacheName, marshaller, cacheMode);
      this.numOwners = numOwners;
   }

   CompatibilityCacheFactory<K, V> keyEquivalence(Equivalence equivalence) {
      this.keyEquivalence = equivalence;
      return this;
   }

   CompatibilityCacheFactory<K, V> valueEquivalence(Equivalence equivalence) {
      this.valueEquivalence = equivalence;
      return this;
   }

   CompatibilityCacheFactory<K, V> setup() throws Exception {
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

      if (cacheMode.isDistributed() && numOwners != defaultNumOwners) {
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

      embeddedCache = cacheName.isEmpty()
            ? cacheManager.<K, V>getCache()
            : cacheManager.<K, V>getCache(cacheName);
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
            .addJavaSerialWhiteList(".*Person.*", ".*CustomEvent.*")
            .marshaller(marshaller)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.<K, V>getCache()
            : hotrodClient.<K, V>getCache(cacheName);
   }

   void createRestCache(int port) throws Exception {
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.port(port);
      rest = NettyRestServer.apply(builder.build(), cacheManager);
      rest.start();
      restClient = new HttpClient();
   }

   private void createMemcachedCache(int port) {
      memcached = startMemcachedTextServer(cacheManager, port);
      memcachedClient = createMemcachedClient(60000, memcached.getPort());
   }

   static void killCacheFactories(CompatibilityCacheFactory... cacheFactories) {
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

   Cache<K, V> getEmbeddedCache() {
      return embeddedCache;
   }

   RemoteCache<K, V> getHotRodCache() {
      return hotrodCache;
   }

   int getHotRodPort() {
      return hotrod.getPort();
   }

   HttpClient getRestClient() {
      return restClient;
   }

   MemcachedClient getMemcachedClient() {
      return memcachedClient;
   }

   int getMemcachedPort() {
      return memcached.getPort();
   }

   String getRestUrl() {
      String restCacheName = cacheName.isEmpty() ? BasicCacheContainer.DEFAULT_CACHE_NAME : cacheName;
      return String.format("http://localhost:%s/rest/%s", restPort, restCacheName);
   }

   HotRodServer getHotrodServer() {
      return hotrod;
   }

}
