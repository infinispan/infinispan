package org.infinispan.server.test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCounterManagerFactory;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.test.Exceptions;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerTestMethodRule implements TestRule {
   private final InfinispanServerRule infinispanServerRule;
   private String methodName;
   private List<Closeable> resources;

   public InfinispanServerTestMethodRule(InfinispanServerRule infinispanServerRule) {
      assert infinispanServerRule != null;
      this.infinispanServerRule = infinispanServerRule;
   }

   public <T extends Closeable> T registerResource(T resource) {
      resources.add(resource);
      return resource;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            before();
            try {
               methodName = description.getMethodName();
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before() {
      resources = new ArrayList<>();
   }

   private void after() {
      resources.forEach(closeable -> Util.close(closeable));
      resources.clear();
   }

   public String getMethodName() {
      return methodName;
   }

   public <K, V> RemoteCache<K, V> getHotRodCache(CacheMode mode) {
      return getHotRodCache(new ConfigurationBuilder(), mode);
   }

   public <K, V> RemoteCache<K, V> getHotRodCache(ConfigurationBuilder builder, CacheMode mode) {
      RemoteCacheManager remoteCacheManager = registerResource(infinispanServerRule.newHotRodClient(builder));
      return remoteCacheManager.administration().getOrCreateCache(methodName, "org.infinispan." + mode.name());
   }

   public RestClient getRestClient(CacheMode mode) {
      return getRestClient(new RestClientConfigurationBuilder(), mode);
   }

   public RestClient getRestClient(RestClientConfigurationBuilder builder, CacheMode mode) {
      RestClient restClient = registerResource(infinispanServerRule.newRestClient(builder));
      Exceptions.unchecked(() -> restClient.createCacheFromTemplate(methodName, "org.infinispan." + mode.name()).toCompletableFuture().get(5, TimeUnit.SECONDS));
      return restClient;
   }

   public CounterManager getCounterManager() {
      RemoteCacheManager remoteCacheManager = registerResource(infinispanServerRule.newHotRodClient());
      return RemoteCounterManagerFactory.asCounterManager(remoteCacheManager);
   }

   public MemcachedClient getMemcachedClient() {
      return registerResource(infinispanServerRule.newMemcachedClient()).getClient();
   }
}
