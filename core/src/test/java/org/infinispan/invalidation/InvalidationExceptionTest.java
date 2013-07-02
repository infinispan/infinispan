package org.infinispan.invalidation;

import org.infinispan.AdvancedCache;
import org.infinispan.config.Configuration;
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
      Configuration invalidAsync = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      createClusteredCaches(2, "invalidAsync", invalidAsync);

      Configuration replQueue = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      replQueue.setUseReplQueue(true);
      defineConfigurationOnAllManagers("invalidReplQueueCache", replQueue);

      Configuration asyncMarshall = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      asyncMarshall.setUseAsyncMarshalling(true);
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
