package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.loaders.cluster.ClusterCacheLoader}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.ClusterCacheLoaderTest")
public class ClusterCacheLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder config1 = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      config1.loaders().addClusterCacheLoader();

      ConfigurationBuilder config2 = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false);
      config2.loaders().addClusterCacheLoader();
      config2.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);

      cacheManager1.defineConfiguration("clusteredCl", config1.build());
      cacheManager2.defineConfiguration("clusteredCl", config2.build());
      waitForClusterToForm("clusteredCl");
   }

   public void testRemoteLoad() {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      assert cache1.get("key") == null;
      assert cache1.get("key") == null;
      cache2.put("key", "value");
      assert "value".equals(cache1.get("key"));
   }

   public void testRemoteLoadFromCacheLoader() throws Exception {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      CacheLoaderManager manager2 = cache2.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      ChainingCacheStore chainingCacheStore = (ChainingCacheStore) manager2.getCacheStore();
      CacheStore cs2 = chainingCacheStore.getStores().keySet().iterator().next();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
      cs2.store(TestInternalCacheEntryFactory.create("key", "value"));
      assert cs2.load("key").getValue().equals("value");
      assert cache1.get("key").equals("value");
   }
}
