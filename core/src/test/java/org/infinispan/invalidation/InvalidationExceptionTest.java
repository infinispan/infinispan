package org.infinispan.invalidation;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.replication.ReplicationExceptionTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

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

      ConfigurationBuilder replQueue = getDefaultClusteredCacheConfig(
            CacheMode.INVALIDATION_ASYNC, false);
      replQueue.clustering().async().useReplQueue(true);
      defineConfigurationOnAllManagers("invalidReplQueueCache", replQueue);

      ConfigurationBuilder asyncMarshall = getDefaultClusteredCacheConfig(
            CacheMode.INVALIDATION_ASYNC, false);

      asyncMarshall.clustering().async().asyncMarshalling();
      defineConfigurationOnAllManagers("invalidAsyncMarshallCache", asyncMarshall);
   }

   public void testNonSerializableAsyncInvalid() throws Exception {
      doNonSerializableInvalidTest("invalidAsync");
   }

   public void testNonSerializableReplQueue() throws Exception {
      doNonSerializableInvalidTest("invalidReplQueueCache");
   }

   public void testNonSerializableAsyncMarshalling() throws Exception {
      doNonSerializableInvalidTest("invalidAsyncMarshallCache");
   }

   private void doNonSerializableInvalidTest(String cacheName) {
      AdvancedCache cache1 = cache(0, cacheName).getAdvancedCache();
      AdvancedCache cache2 = cache(1, cacheName).getAdvancedCache();
      try {
         cache1.put(new ReplicationExceptionTest.ContainerData(), "test-" + cacheName);
         // We should not come here.
         assertNotNull("NonSerializableData should not be null on cache2", cache2.get("test"));
      } catch (RuntimeException runtime) {
         Throwable t = runtime.getCause();
         if (runtime instanceof NotSerializableException
                  || t instanceof NotSerializableException
                  || t.getCause() instanceof NotSerializableException) {
            log.trace("received NotSerializableException - as expected");
         } else {
            throw runtime;
         }
      }
   }

}
