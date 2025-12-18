package org.infinispan.server.test.core;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Eventually;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.JmxTestClient;
import org.infinispan.server.test.api.MemcachedTestClientDriver;
import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

/**
 * Holds the client part of the testing utilities
 *
 * @author Katia Aresti
 * @since 11
 */
public class TestClient {
   protected InfinispanServerTestConfiguration configuration;
   protected TestServer testServer;
   protected List<AutoCloseable> resources;
   protected Map<String, RemoteCacheManager> hotrodCacheMap;
   protected Map<String, RestClient> restCacheMap;
   private String methodName;

   public TestClient(TestServer testServer) {
      this.testServer = testServer;
   }

   public <T extends AutoCloseable> T registerResource(T resource) {
      resources.add(resource);
      return resource;
   }

   public void registerHotRodCache(String name, RemoteCacheManager remoteCacheManager) {
      hotrodCacheMap.put(name, remoteCacheManager);
   }

   public void registerRestCache(String name, RestClient restClient) {
      restCacheMap.put(name, restClient);
   }

   public InfinispanServerDriver getServerDriver() {
      if (!testServer.isDriverInitialized()) {
         throw new IllegalStateException("Operation not supported before test starts");
      }
      return testServer.getDriver();
   }

   public HotRodTestClientDriver hotrod() {
      return new HotRodTestClientDriver(testServer, this);
   }

   public RestTestClientDriver rest() {
      return new RestTestClientDriver(testServer, this);
   }

   public RespTestClientDriver resp() {
      return new RespTestClientDriver(testServer, this);
   }

   public MemcachedTestClientDriver memcached() {
      return new MemcachedTestClientDriver(testServer, this);
   }

   public JmxTestClient jmx() {
      return new JmxTestClient(testServer, this);
   }

   public CounterManager getCounterManager() {
      RemoteCacheManager remoteCacheManager = registerResource(hotrod().createRemoteCacheManager());
      return RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
   }

   public <K, V> MultimapCacheManager<K, V> getRemoteMultimapCacheManager() {
      RemoteCacheManager remoteCacheManager = registerResource(hotrod().createRemoteCacheManager());
      return RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
   }

   public void setMethodName(String methodName) {
      this.methodName = methodName;
   }

   public void clearResources() {
      if (hotrodCacheMap != null) {
         hotrodCacheMap.forEach( (n, rcm) -> rcm.administration().removeCache(n));
         hotrodCacheMap.clear();
      }
      if (restCacheMap != null) {
         restCacheMap.forEach( (n, rc) -> rc.cache(n).delete());
         restCacheMap.clear();
      }
      if (resources != null) {
         ExecutorService executor = Executors.newSingleThreadExecutor();
         try {
            CompletableFuture
                  .allOf(
                        resources.stream()
                              .map(resource -> CompletableFuture.runAsync(() -> Util.close(resource), executor))
                              .toArray(i -> new CompletableFuture<?>[i])
                  )
                  .get(Eventually.DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
         } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new IllegalStateException("Unable to close resources for " + methodName, e);
         } finally {
            resources.clear();
            executor.shutdown();
         }
      }
   }

   public void initResources() {
      resources = new ArrayList<>();
      hotrodCacheMap = new HashMap<>();
      restCacheMap = new HashMap<>();
   }

   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE_NAME);
      try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(script)) {
         scriptCache.put(getMethodName(), CommonsTestingUtil.loadFileAsString(in));
      } catch (HotRodClientException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return getMethodName();
   }

   public RestClient newRestClient(RestClientConfigurationBuilder restClientConfigurationBuilder) {
      RestClient restClient = testServer.newRestClient(restClientConfigurationBuilder);
      registerResource(restClient);
      return restClient;
   }

   public MemcachedClient getMemcachedClient(ConnectionFactoryBuilder builder) {
      TestServer.CloseableMemcachedClient memcachedClient = testServer.newMemcachedClient(builder);
      return registerResource(memcachedClient).client();
   }

   public String getMethodName() {
      return getMethodName((Object) null);
   }

   public String getMethodName(Object... qualifiers) {
      StringBuilder sb = new StringBuilder("C").append(methodName);
      if (qualifiers != null) {
         for (Object q : qualifiers) {
            if (q != null)
               sb.append(q);
         }
      }
      String cacheName = sb.toString();
      try {
         MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
         byte[] digest = sha1.digest(cacheName.getBytes(StandardCharsets.UTF_8));
         return Util.toHexString(digest);
      } catch (NoSuchAlgorithmException e) {
         // Won't happen
         return null;
      }
   }
}
