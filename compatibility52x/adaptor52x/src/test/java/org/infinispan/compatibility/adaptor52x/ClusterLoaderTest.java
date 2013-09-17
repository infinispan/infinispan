package org.infinispan.compatibility.adaptor52x;

import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "compatibility.adaptor52x.ClusterLoaderTest")
public class ClusterLoaderTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager("52x-cluster-loader.xml");
   }

   public void testLoaders() {
      List<StoreConfiguration> stores = cacheManager.getCache("customLoaderCache").getCacheConfiguration().persistence().stores();
      assertEquals(stores.size(), 1);
      StoreConfiguration storeConfiguration = stores.get(0);
      assertTrue(storeConfiguration instanceof ClusterLoaderConfiguration);
      ClusterLoaderConfiguration csc = (ClusterLoaderConfiguration) storeConfiguration;
      assertEquals(csc.remoteCallTimeout(), 1222);
   }
}
