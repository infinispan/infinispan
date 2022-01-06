package org.infinispan.rest.cachemanager;

import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.framework.impl.SimpleRequest;
import org.infinispan.server.core.CacheInfo;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.RestCacheManagerTest")
public class RestCacheManagerTest extends SingleCacheManagerTest {

   @BeforeClass
   public void prepare() {
      Configuration config = new ConfigurationBuilder().build();
      cacheManager.createCache("cache1", config);
      cacheManager.createCache("cache2", config);
   }

   @Test
   public void shouldReuseEncodedCaches() {
      EmbeddedCacheManager embeddedCacheManager = Mockito.spy(cacheManager);
      RestCacheManager<Object> restCacheManager = new RestCacheManager<>(embeddedCacheManager, c -> Boolean.FALSE);
      Map<String, CacheInfo<Object, Object>> knownCaches = TestingUtil.extractField(restCacheManager, "knownCaches");

      // Request cache by simple name
      SimpleRequest request = new SimpleRequest.Builder().setPath("/test").build();
      restCacheManager.getCache("cache1", request);
      restCacheManager.getCache("cache2", request);

      // Verify they are stored internally
      assertEquals(knownCaches.size(), 2);
      assertEquals(cachesSize(knownCaches.get("cache1")), 1);
      assertEquals(cachesSize(knownCaches.get("cache2")), 1);

      // Requesting again should not cause interaction with the cache manager
      Mockito.reset(embeddedCacheManager);
      restCacheManager.getCache("cache1", request);
      restCacheManager.getCache("cache2", request);

      Mockito.verify(embeddedCacheManager, never()).getCache("cache1");
      Mockito.verify(embeddedCacheManager, never()).getCache("cache2");
      assertEquals(cachesSize(knownCaches.get("cache1")), 1);
      assertEquals(cachesSize(knownCaches.get("cache2")), 1);

      // Request caches with a different media type
      restCacheManager.getCache("cache1", MediaType.MATCH_ALL, MediaType.APPLICATION_JSON, request);
      restCacheManager.getCache("cache2", MediaType.MATCH_ALL, MediaType.TEXT_PLAIN, request);

      // Verify they are stored internally
      assertEquals(knownCaches.size(), 2);
      assertEquals(cachesSize(knownCaches.get("cache1")), 2);
      assertEquals(cachesSize(knownCaches.get("cache2")), 2);

      // Requesting again with same media type but different parameters should not reuse internal instance
      Mockito.reset(embeddedCacheManager);
      restCacheManager.getCache("cache1", MediaType.MATCH_ALL, MediaType.fromString("application/json; charset=UTF-8"), request);
      restCacheManager.getCache("cache2", MediaType.MATCH_ALL, MediaType.fromString("text/plain; charset=SHIFT-JIS"), request);

      assertEquals(knownCaches.size(), 2);
      assertEquals(cachesSize(knownCaches.get("cache1")), 3);
      assertEquals(cachesSize(knownCaches.get("cache2")), 3);
      Mockito.verify(embeddedCacheManager, never()).getCache("cache1");
      Mockito.verify(embeddedCacheManager, never()).getCache("cache2");

      // Requesting with same params should reuse
      restCacheManager.getCache("cache1", MediaType.MATCH_ALL, MediaType.fromString("application/json; charset=UTF-8"), request);
      restCacheManager.getCache("cache2", MediaType.MATCH_ALL, MediaType.fromString("text/plain; charset=SHIFT-JIS"), request);

      assertEquals(cachesSize(knownCaches.get("cache1")), 3);
      assertEquals(cachesSize(knownCaches.get("cache2")), 3);
      Mockito.verify(embeddedCacheManager, never()).getCache("cache1");
      Mockito.verify(embeddedCacheManager, never()).getCache("cache2");
   }

   private int cachesSize(CacheInfo<Object, Object> cacheInfo) {
      Map<?, ?> caches = TestingUtil.extractField(cacheInfo, "encodedCaches");
      return caches.size();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(false));
   }
}
