package org.infinispan.invalidation;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.replication.ReplicationExceptionTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test to verify how the invalidation works under exceptional circumstances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "invalidation.InvalidationExceptionTest")
public class InvalidationExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder invalidAsync = getDefaultClusteredCacheConfig(
            CacheMode.INVALIDATION_ASYNC, false);
      createClusteredCaches(2, "invalidAsync", invalidAsync);
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonSerializableAsyncInvalid() {
      String cacheName = "invalidAsync";
      AdvancedCache cache1 = cache(0, cacheName).getAdvancedCache();
      cache1.put(new ReplicationExceptionTest.ContainerData(), "test-" + cacheName);
   }
}
