package org.infinispan.client.hotrod.near;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.mockito.Mockito;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "client.hotrod.near.InvalidationNearCacheConcurrencyTest")
public class InvalidationNearCacheConcurrencyTest extends SingleHotRodServerTest {
   private static final String CACHE_NAME = InvalidationNearCacheConcurrencyTest.class.getName();

   private boolean bloomFilter;

   InvalidationNearCacheConcurrencyTest bloomFilter(boolean enabled) {
      this.bloomFilter = enabled;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new InvalidationNearCacheConcurrencyTest().bloomFilter(true),
            new InvalidationNearCacheConcurrencyTest().bloomFilter(false),
      };
   }

   @Override
   protected String parameters() {
      return "[bloomFilter = " + bloomFilter + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder cb = hotRodCacheConfiguration();
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(cb);
      manager.createCache(CACHE_NAME, cb.build());
      return manager;
   }

   @Override
   protected ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int port) {
      ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(host, port);
      builder.remoteCache(CACHE_NAME).nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheUseBloomFilter(bloomFilter)
            .nearCacheMaxEntries(100);
      return builder;
   }

   public void testConcurrentInvalidationWithRetrieval() throws InterruptedException, TimeoutException, ExecutionException {
      RemoteCache<Integer, String> remoteCache = remoteCacheManager.getCache(CACHE_NAME);
      remoteCache.put(1, "foo");
      Cache<?, ?> cache = cacheManager.getCache(CACHE_NAME);
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      // Block on the checkpoint when it is requesting segment 2 from node 2 (need both as different methods are invoked
      // if the invocation is parallel)
      InternalDataContainer<?, ?> realContainer = Mocks.blockingMock(checkPoint, InternalDataContainer.class, cache,
            (stub, m) -> stub.when(m).peek(Mockito.anyInt(), Mockito.any()));

      Future<Void> future = fork(() -> assertEquals("foo", remoteCache.get(1)));

      checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // Put back the original so we don't block the remove below
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, realContainer, true);

      // Remove the key
      cache.keySet().forEach(cache::remove);

      checkPoint.trigger(Mocks.AFTER_RELEASE);

      future.get(10, TimeUnit.SECONDS);

      assertEquals(0, cache.size());

      eventuallyEquals(null, () -> remoteCache.get(1));
   }
}
