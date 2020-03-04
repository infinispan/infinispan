package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.server.hotrod.test.TestSizeResponse;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodWithStoreTest")
public class HotRodWithStoreTest extends HotRodSingleNodeTest {

   @Override
   public EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.persistence()
             .addStore(DummyInMemoryStoreConfigurationBuilder.class)
             .storeName(getClass().getName());
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(
            new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName(cacheName),
            builder);

      advancedCache = cacheManager.<byte[], byte[]>getCache(cacheName).getAdvancedCache();
      return cacheManager;
   }

   public void testSize(Method m) {
      TestSizeResponse sizeStart = client().size();
      assertStatus(sizeStart, Success);
      assertEquals(0, sizeStart.size);
      for (int i = 0; i < 20; i++)
         client().assertPut(m, "k-" + i, "v-" + i);

      // Clear contents from memory
      advancedCache.withFlags(Flag.SKIP_CACHE_STORE).clear();

      TestSizeResponse sizeEnd = client().size();
      assertStatus(sizeEnd, Success);
      assertEquals(20, sizeEnd.size);
   }

}
