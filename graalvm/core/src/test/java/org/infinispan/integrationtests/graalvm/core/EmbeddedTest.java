package org.infinispan.integrationtests.graalvm.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.jupiter.api.Test;

public class EmbeddedTest {

   @Test
   public void programmaticCacheTest() throws Exception {
      try (EmbeddedCacheManager cm = new DefaultCacheManager()) {
         Cache<String, String> cache = cm.createCache("local-cache", new ConfigurationBuilder().build());
         testCacheOperations(cache);
      }
   }

   @Test
   public void xmlConfigTest() throws Exception {
      try (EmbeddedCacheManager cm = new DefaultCacheManager("embedded.xml")) {
         for (String cacheName : cm.getCacheNames()) {
            Cache<String, String> cache = cm.getCache(cacheName);
            testCacheOperations(cache);
         }
      }
   }

   @Test
   public void clusteredCacheTest() {
      TestResourceTracker.setThreadTestName("clusteredCacheTest");
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);

      List<EmbeddedCacheManager> cacheManagers = new ArrayList<>(3);
      List<Cache<String, String>> caches = new ArrayList<>(3);
      for (int i = 0; i < 3; i++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(config);
         cacheManagers.add(cm);
         caches.add(cm.getCache());
      }
      TestingUtil.blockUntilViewsReceived(30000, caches);
      TestingUtil.waitForNoRebalance(caches);

      Cache<String, String> cache = caches.get(0);
      testCacheOperations(cache);
   }

   private void testCacheOperations(Cache<String, String> cache) {
      String cacheName = cache.getName();
      String key = "1";
      cache.put(key, "1");
      assertEquals(1, cache.size(), cacheName);
      assertEquals("1", cache.get(key), cacheName);
      assertEquals("1", cache.remove(key), cacheName);
      assertNull(cache.get(key), cacheName);
   }
}
