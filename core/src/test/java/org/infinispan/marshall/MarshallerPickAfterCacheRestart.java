package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests the marshaller is picked correctly when a cache is restarted.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "marshall.MarshallerPickAfterCacheRestart")
public class MarshallerPickAfterCacheRestart extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storageType(StorageType.BINARY)
            .clustering()
               .cacheMode(CacheMode.REPL_SYNC)
               .stateTransfer().fetchInMemoryState(false);

      createCluster(builder, 2);
   }

   public void testCacheRestart() throws Exception {
      final Cache<Integer, String> cache0 = cache(0);
      final Cache<Integer, String> cache1 = cache(1);

      // Restart the cache
      cache1.stop();
      cache1.start();

      cache1.put(1, "value1");
      assertEquals("value1", cache0.get(1));
   }

}
