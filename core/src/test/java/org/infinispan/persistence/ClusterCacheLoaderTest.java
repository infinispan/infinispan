package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tester for {@link org.infinispan.persistence.cluster.ClusterLoader}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.ClusterCacheLoaderTest")
public class ClusterCacheLoaderTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder config1 = getDefaultClusteredCacheConfig(cacheMode(), false);
      config1.persistence().addClusterLoader();

      ConfigurationBuilder config2 = getDefaultClusteredCacheConfig(cacheMode(), false);
      config2.persistence().addClusterLoader();
      config2.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

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
      CacheWriter writer = TestingUtil.getFirstWriter(cache2);


      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
      writer.write(new MarshalledEntryImpl("key", "value", null, cache2.getAdvancedCache().getComponentRegistry().getCacheMarshaller()));
      assert ((CacheLoader)writer).load("key").getValue().equals("value");
      assert cache1.get("key").equals("value");
   }

   protected CacheMode cacheMode() {
      return CacheMode.INVALIDATION_SYNC;
   }
}
