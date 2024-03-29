package org.infinispan.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.persistence.cluster.ClusterLoader}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.ClusterCacheLoaderTest")
@InCacheMode({ CacheMode.INVALIDATION_SYNC, CacheMode.DIST_SYNC, CacheMode.REPL_SYNC })
public class ClusterCacheLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder config1 = getDefaultClusteredCacheConfig(cacheMode, false);
      config1.persistence().addClusterLoader()
            .segmented(false);

      ConfigurationBuilder config2 = getDefaultClusteredCacheConfig(cacheMode, false);
      config2.persistence().addClusterLoader()
            .segmented(false);
      config2.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

      cacheManager1.defineConfiguration("clusteredCl", config1.build());
      cacheManager2.defineConfiguration("clusteredCl", config2.build());
      waitForClusterToForm("clusteredCl");
   }

   public void testRemoteLoad() {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      assertNull(cache1.get("key"));
      assertNull(cache1.get("key"));
      cache2.put("key", "value");
      assertEquals(cache1.get("key"), "value");
   }

   public void testRemoteLoadFromCacheLoader() throws Exception {
      Cache<String, String> cache1 = cache(0, "clusteredCl");
      Cache<String, String> cache2 = cache(1, "clusteredCl");
      DummyInMemoryStore writer = TestingUtil.getStore(cache2, 1, false);

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));

      writer.write(MarshalledEntryUtil.create("key", "value", cache2));
      assertEquals(writer.loadEntry("key").getValue(), "value");
      assertEquals(cache1.get("key"), "value");
   }
}
