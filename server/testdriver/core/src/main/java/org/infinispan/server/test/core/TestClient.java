package org.infinispan.server.test.core;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.api.RestTestClientDriver;

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
   protected List<Closeable> resources;
   private String methodName;

   public TestClient(TestServer testServer) {
      this.testServer = testServer;
   }

   public <T extends Closeable> T registerResource(T resource) {
      resources.add(resource);
      return resource;
   }

   public InfinispanServerDriver getServerDriver() {
      if (!testServer.isDriverInitialized()) {
         throw new IllegalStateException("Operation not supported before test starts");
      }
      return testServer.getDriver();
   }

   public HotRodTestClientDriver hotrod() {
      return hotrod(configurationBuilder -> {});
   }

   public HotRodTestClientDriver hotrod(Consumer<ConfigurationBuilder> additionalConfigurations) {
      return new HotRodTestClientDriver(testServer, this, additionalConfigurations);
   }

   public RestTestClientDriver rest() {
      return new RestTestClientDriver(testServer, this);
   }

   public CounterManager getCounterManager() {
      RemoteCacheManager remoteCacheManager = registerResource(testServer.newHotRodClient());
      return RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
   }

   public <K, V> MultimapCacheManager<K, V> getRemoteMultimapCacheManager() {
      RemoteCacheManager remoteCacheManager = registerResource(testServer.newHotRodClient());
      return RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
   }

   public void setMethodName(String methodName) {
      this.methodName = methodName;
   }

   public void clearResources() {
      if (resources != null) {
         resources.forEach(Util::close);
         resources.clear();
      }
   }

   public void initResources() {
      resources = new ArrayList<>();
   }

   public String addScript(RemoteCacheManager remoteCacheManager, String script) {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(ScriptingManager.SCRIPT_CACHE);
      try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(script)) {
         scriptCache.put(getMethodName(), CommonsTestingUtil.loadFileAsString(in));
      }  catch (HotRodClientException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return getMethodName();
   }

   public String getMethodName() {
      return getMethodName(null);
   }

   public RestClient newRestClient(RestClientConfigurationBuilder restClientConfigurationBuilder) {
      RestClient restClient = testServer.newRestClient(restClientConfigurationBuilder);
      registerResource(restClient);
      return restClient;
   }

   public String getMethodName(String qualifier) {
      String cacheName = "C" + methodName + (qualifier != null ? qualifier : "");
      try {
         MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
         byte[] digest = sha1.digest(cacheName.getBytes(StandardCharsets.UTF_8));
         return Util.toHexString(digest);
      } catch (NoSuchAlgorithmException e) {
         // Won't happen
         return null;
      }
   }

   public MemcachedClient getMemcachedClient() {
      return registerResource(testServer.newMemcachedClient()).getClient();
   }
}
