package org.infinispan.partitionhandling;

import static org.infinispan.test.Mocks.invokeAndReturnMock;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

/**
 * Tests to make sure that distributed stream pays attention to partition status
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "partitionhandling.StreamDistPartitionHandlingTest")
public class StreamDistPartitionHandlingTest extends BasePartitionHandlingTest {
   @Test( expectedExceptions = AvailabilityException.class)
   public void testRetrievalWhenPartitionIsDegraded() {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      try (CloseableIterator iterator = Closeables.iterator(cache(0).entrySet().stream())) {
         iterator.next();
      }
   }

   public void testRetrievalWhenPartitionIsDegradedButLocal() {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      try (CloseableIterator<Map.Entry<MagicKey, String>> iterator = Closeables.iterator(cache0.getAdvancedCache()
              .withFlags(Flag.CACHE_MODE_LOCAL).entrySet().stream())) {
         assertEquals("local", iterator.next().getValue());
         assertFalse(iterator.hasNext());
      }
   }

   @Test(enabled = false)
   public void testUsingIterableButPartitionOccursBeforeGettingIterator() throws InterruptedException {
      // Repl only checks for partition when first retrieving the entrySet, keySet or values
   }

   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws InterruptedException {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      CheckPoint cp = new CheckPoint();
      // This must be before the stream is generated or else it won't see the update
      blockStreamResponse(cp, cache0);
      // we have to enable parallel distribution since single distribution uses the async thread to send requests and
      // it can't complete since we are blocking the response
      try (CloseableIterator<?> iterator = Closeables.iterator(cache0.entrySet().stream().parallelDistribution())) {

         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(cp, cache0);

         // We don't want to block the notifier
         cp.triggerForever("pre_notify_partition_released");
         cp.triggerForever("post_notify_partition_released");

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Wait until we have been notified before letting remote responses to arrive
         assertTrue(cp.await("post_notify_partition_invoked", 10, TimeUnit.SECONDS));

         // Afterwards let all the responses come in
         cp.triggerForever("pre_receive_response_released");
         cp.triggerForever("post_receive_response_released");

         try {
            while (iterator.hasNext()) {
               iterator.next();
            }
            fail("Expected AvailabilityException");
         } catch (AvailabilityException e) {
            // Should go here
         }
      }
   }

   public void testUsingIteratorButPartitionOccursAfterRetrievingRemoteValues() throws InterruptedException {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      CheckPoint cp = new CheckPoint();
      // This must be before the stream is generated or else it won't see the update
      blockStreamResponse(cp, cache0);
      // we have to enable parallel distribution since single distribution uses the async thread to send requests and
      // it can't complete since we are blocking the response
      try (CloseableIterator<?> iterator = Closeables.iterator(cache0.entrySet().stream().parallelDistribution())) {

         // Let all the responses go first
         cp.triggerForever("pre_receive_response_released");
         cp.triggerForever("post_receive_response_released");

         // Wait for all the responses to come back - we have to do this before splitting
         assertTrue(cp.await("post_receive_response_invoked", numMembersInCluster - 1, 10, TimeUnit.SECONDS));

         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(cp, cache0);

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Now let the notification occur after all the responses are done
         cp.triggerForever("pre_notify_partition_released");
         cp.triggerForever("post_notify_partition_released");

         // This should complete without issue now
         while (iterator.hasNext()) {
            iterator.next();
         }
      }
   }

   private static <K, V> CacheNotifier<K, V> blockNotifierPartitionStatusChanged(final CheckPoint checkPoint,
                                                                                 Cache<K, V> cache) {
      CacheNotifier<K, V> notifier = TestingUtil.extractComponent(cache, CacheNotifier.class);
      CacheNotifier mockNotifier =
            mock(CacheNotifier.class, withSettings().defaultAnswer(i -> invokeAndReturnMock(i, notifier)));

      doAnswer(invocation -> {
         checkPoint.trigger("pre_notify_partition_invoked");
         assertTrue(checkPoint.await("pre_notify_partition_released", 20, TimeUnit.SECONDS));
         try {
            return invokeAndReturnMock(invocation, notifier);
         } finally {
            checkPoint.trigger("post_notify_partition_invoked");
            assertTrue(checkPoint.await("post_notify_partition_released", 20, TimeUnit.SECONDS));
         }
      }).when(mockNotifier).notifyPartitionStatusChanged(eq(AvailabilityMode.DEGRADED_MODE), eq(false));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
      return notifier;
   }

   private static <K> ClusterStreamManager<K> blockStreamResponse(final CheckPoint checkPoint, Cache<K, ?> cache) {
      ClusterStreamManager<K> manager = TestingUtil.extractComponent(cache, ClusterStreamManager.class);
      ClusterStreamManager mockManager = mock(ClusterStreamManager.class, withSettings().defaultAnswer(i -> {
         try {
            return invokeAndReturnMock(i, manager);
         } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof AvailabilityException) {
               throw cause;
            }
            throw e;
         }
      }));
      doAnswer(invocation -> {
         checkPoint.trigger("pre_receive_response_invoked");
         assertTrue(checkPoint.await("pre_receive_response_released", 20, TimeUnit.SECONDS));
         try {
            return invokeAndReturnMock(invocation, manager);
         } finally {
            checkPoint.trigger("post_receive_response_invoked");
            assertTrue(checkPoint.await("post_receive_response_released", 20, TimeUnit.SECONDS));
         }
      }).when(mockManager).receiveResponse(any(String.class), any(Address.class), anyBoolean(), anySet(), any());
      TestingUtil.replaceComponent(cache, ClusterStreamManager.class, mockManager, true);
      return manager;
   }
}
