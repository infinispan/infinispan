package org.infinispan.query.helper;

import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

public class TestableCluster<K, V> {

   private final List<EmbeddedCacheManager> cacheManagers = new ArrayList<>();
   private final List<Cache<K, V>> caches = new ArrayList<>();
   private final String configurationResourceName;

   public TestableCluster(String configurationResourceName) {
      this.configurationResourceName = configurationResourceName;
   }

   public synchronized EmbeddedCacheManager startNewNode() {
      EmbeddedCacheManager cacheManager;
      try {
         cacheManager = TestCacheManagerFactory.fromXml(configurationResourceName);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      cacheManagers.add(cacheManager);
      Cache<K, V> cache = cacheManager.getCache();
      caches.add(cache);
      waitForStableTopology(cache, caches);
      return cacheManager;
   }

   public synchronized Cache<K, V> getCache(int nodeId) {
      return caches.get(nodeId);
   }

   public synchronized void killAll() {
      TestingUtil.killCacheManagers(cacheManagers);
      caches.clear();
      cacheManagers.clear();
   }

   public synchronized Iterable<Cache<K, V>> iterateAllCaches() {
      return new ArrayList<Cache<K, V>>(caches);
   }

   public synchronized void killNode(Cache<K, V> cache) {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      TestingUtil.killCacheManagers(cacheManager);
      assertTrue(caches.remove(cache));
      assertTrue(cacheManagers.remove(cacheManager));
      waitForStableTopology(cache, caches);
   }

   private static <K, V> void waitForStableTopology(Cache<K, V> cache, List<Cache<K, V>> caches) {
      if (cache.getCacheConfiguration().clustering().cacheMode() != CacheMode.LOCAL) {
         TestingUtil.waitForStableTopology(caches);
      }
   }

}
