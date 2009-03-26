package org.horizon.loader;

import org.horizon.Cache;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.container.entries.InternalEntryFactory;
import org.horizon.loader.cluster.ClusterCacheLoaderConfig;
import org.horizon.loader.decorators.ChainingCacheStore;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.loader.jdbc.UnitTestDatabaseManager;
import org.horizon.loader.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tester for {@link ClusterCacheLoader}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loader.ClusterCacheLoaderTest")
public class ClusterCacheLoaderTest extends MultipleCacheManagersTest {

   private Cache cache1;
   private Cache cache2;
   private CacheStore cs2;

   protected void createCacheManagers() throws Throwable {
      CacheManager cacheManager1 = new DefaultCacheManager(TestingUtil.getGlobalConfiguration());
      CacheManager cacheManager2 = new DefaultCacheManager(TestingUtil.getGlobalConfiguration());
      registerCacheManager(cacheManager1, cacheManager2);

      Configuration config1 = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      ClusterCacheLoaderConfig clusterClc = new ClusterCacheLoaderConfig();
      CacheLoaderManagerConfig clMngrConfig = new CacheLoaderManagerConfig();
      clMngrConfig.addCacheLoaderConfig(clusterClc);
      config1.setCacheLoaderManagerConfig(clMngrConfig);

      Configuration config2 = getDefaultClusteredConfig(Configuration.CacheMode.INVALIDATION_SYNC);
      CacheLoaderManagerConfig clMngrConfig2 = clMngrConfig.clone();//this also includes the clustered CL
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcBinaryCacheStoreConfig jdmcClConfig = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tm);
      clMngrConfig2.addCacheLoaderConfig(jdmcClConfig);
      assert clMngrConfig2.getCacheLoaderConfigs().size() == 2;
      config2.setCacheLoaderManagerConfig(clMngrConfig2);


      cacheManager1.defineCache("clusteredCl", config1);
      cacheManager2.defineCache("clusteredCl", config2);
      cache1 = cache(0, "clusteredCl");
      cache2 = cache(1, "clusteredCl");
      CacheLoaderManager manager2 = cache2.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      ChainingCacheStore chainingCacheStore = (ChainingCacheStore) manager2.getCacheStore();
      cs2 = chainingCacheStore.getStores().keySet().iterator().next();
   }

   public void testRemoteLoad() {
      assert cache1.get("key") == null;
      assert cache1.get("key") == null;
      cache2.put("key", "value");
      assert cache1.get("key").equals("value");
   }

   public void testRemoteLoadFromCacheLoader() throws Exception {
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
      cs2.store(InternalEntryFactory.create("key", "value"));
      assert cs2.load("key").getValue().equals("value");
      assert cache1.get("key").equals("value");
   }
}
