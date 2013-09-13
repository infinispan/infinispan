package org.infinispan.it.compatibility;

import net.spy.memcached.MemcachedClient;
import org.apache.commons.httpclient.HttpClient;
import org.infinispan.Cache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.LegacyMarshallerAdapter;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.rest.ServerBootstrap;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import javax.servlet.ServletContext;
import java.util.Collections;

import static org.infinispan.client.hotrod.TestHelper.startHotRodServer;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
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
   private Server rest;
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
   private Equivalence keyEquivalence = null;
   private Equivalence valueEquivalence = null;

   CompatibilityCacheFactory(CacheMode cacheMode) {
      this.cacheName = "";
      this.marshaller = null;
      this.cacheMode = cacheMode;
   }

   CompatibilityCacheFactory(CacheMode cacheMode, int numOwners) {
      this(cacheMode);
      this.numOwners = numOwners;
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

   @Deprecated
   CompatibilityCacheFactory(String cacheName, org.infinispan.marshall.Marshaller marshaller, CacheMode cacheMode) {
      this.cacheName = cacheName;
      this.marshaller = new LegacyMarshallerAdapter(marshaller);
      this.cacheMode = cacheMode;
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
            .marshaller(marshaller)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.<K, V>getCache()
            : hotrodClient.<K, V>getCache(cacheName);
   }

   void createRestCache(int port) throws Exception {
      rest = new Server(port);
      Context ctx = new Context(rest, "/", Context.SESSIONS);
      ctx.setInitParams(Collections.singletonMap("resteasy.resources", "org.infinispan.rest.Server"));
      ctx.addEventListener(new ResteasyBootstrap());
      ctx.addEventListener(new ServerBootstrap());
      ctx.addServlet(HttpServletDispatcher.class, "/rest/*");
      ServletContext servletContext = ctx.getServletContext();
      ServerBootstrap.setCacheManager(servletContext, cacheManager);
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

   void killRestServer(Server rest) {
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

}
