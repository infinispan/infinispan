package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheManager;
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

   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cacheManager1, cacheManager2);

      Configuration config1 = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      ClusterCacheLoaderConfig clusterClc = new ClusterCacheLoaderConfig();
      CacheLoaderManagerConfig clMngrConfig = new CacheLoaderManagerConfig();
      clMngrConfig.addCacheLoaderConfig(clusterClc);
      config1.setCacheLoaderManagerConfig(clMngrConfig);

      Configuration config2 = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      CacheLoaderManagerConfig clMngrConfig2 = clMngrConfig.clone();//this also includes the clustered CL
      clMngrConfig2.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      assert clMngrConfig2.getCacheLoaderConfigs().size() == 2;
      config2.setCacheLoaderManagerConfig(clMngrConfig2);


      cacheManager1.defineConfiguration("clusteredCl", config1);
      cacheManager2.defineConfiguration("clusteredCl", config2);
      Cache cache1 = cache(0, "clusteredCl");
      Cache cache2 = cache(1, "clusteredCl");
      
   }

   public void testRemoteLoad() {
      Cache cache1 = cache(0, "clusteredCl");
      Cache cache2 = cache(1, "clusteredCl");
      assert cache1.get("key") == null;
      assert cache1.get("key") == null;
      cache2.put("key", "value");
      assert cache1.get("key").equals("value");
   }

   public void testRemoteLoadFromCacheLoader() throws Exception {
      Cache cache1 = cache(0, "clusteredCl");
      Cache cache2 = cache(1, "clusteredCl");
      CacheLoaderManager manager2 = cache2.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      ChainingCacheStore chainingCacheStore = (ChainingCacheStore) manager2.getCacheStore();
      CacheStore cs2 = chainingCacheStore.getStores().keySet().iterator().next();
      
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
      cs2.store(InternalEntryFactory.create("key", "value"));
      assert cs2.load("key").getValue().equals("value");
      assert cache1.get("key").equals("value");
   }
}
