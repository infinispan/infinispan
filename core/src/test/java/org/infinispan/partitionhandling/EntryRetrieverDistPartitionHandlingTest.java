package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.fail;

/**
 * Tests to make sure that entry retriever pays attention to partition status
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "partitionhandling.EntryRetrieverDistPartitionHandlingTest")
public class EntryRetrieverDistPartitionHandlingTest extends BasePartitionHandlingTest {
   @Test( expectedExceptions = AvailabilityException.class)
   public void testRetrievalWhenPartitionIsDegrated() {
      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      try (EntryIterable iterable = cache(0).getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance())) {
         iterable.iterator();
      }
   }

   public void testRetrievalWhenPartitionIsDegratedButLocal() {
      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      try (EntryIterable iterable = cache(0).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).filterEntries(
            AcceptAllKeyValueFilter.getInstance())) {
         iterable.iterator();
      }
   }

   public void testUsingIterableButPartitionOccursBeforeGettingIterator() throws InterruptedException {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");

      try (EntryIterable<MagicKey, String> iterable = cache0.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance())) {
         CheckPoint cp = new CheckPoint();

         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(cp, cache0);

         // We don't want to block the notifier
         cp.triggerForever("pre_notify_partition_released");
         cp.triggerForever("post_notify_partition_released");

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Wait until we have been notified before letting remote responses to arrive
         cp.await("post_notify_partition_invoked", 10, TimeUnit.SECONDS);

         try {
            Iterator<CacheEntry<MagicKey, String>> iterator = iterable.iterator();
            fail("Expected AvailabilityException");
         } catch (AvailabilityException e) {
            // Should go here
         }
      }
   }

   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws InterruptedException {
      Cache<MagicKey, String> cache0 = cache(0);
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      CheckPoint cp = new CheckPoint();
      // This must be before the entry retriever is generated or else it won't see the update
      blockEntryRetrieverResponse(cp, cache0);
      try (CloseableIterator<CacheEntry<MagicKey, String>> iterator = cache0.getAdvancedCache().filterEntries(
            AcceptAllKeyValueFilter.getInstance()).iterator()) {

         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(cp, cache0);

         // We don't want to block the notifier
         cp.triggerForever("pre_notify_partition_released");
         cp.triggerForever("post_notify_partition_released");

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Wait until we have been notified before letting remote responses to arrive
         cp.await("post_notify_partition_invoked", 10, TimeUnit.SECONDS);

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
      // This must be before the entry retriever is generated or else it won't see the update
      blockEntryRetrieverResponse(cp, cache0);
      try (CloseableIterator<CacheEntry<MagicKey, String>> iterator = cache0.getAdvancedCache().filterEntries(
            AcceptAllKeyValueFilter.getInstance()).iterator()) {

         // Let all the responses go first
         cp.triggerForever("pre_receive_response_released");
         cp.triggerForever("post_receive_response_released");

         // Wait for all the responses to come back - we have to do this before splitting
         cp.await("post_receive_response_invoked", numMembersInCluster - 1, 10, TimeUnit.SECONDS);

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
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(notifier);
      CacheNotifier mockNotifier = mock(CacheNotifier.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            checkPoint.trigger("pre_notify_partition_invoked");
            checkPoint.await("pre_notify_partition_released", 10, TimeUnit.SECONDS);
            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               checkPoint.trigger("post_notify_partition_invoked");
               checkPoint.await("post_notify_partition_released", 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockNotifier).notifyPartitionStatusChanged(eq(AvailabilityMode.DEGRADED_MODE), eq(false));
      TestingUtil.replaceComponent(cache, CacheNotifier.class, mockNotifier, true);
      return notifier;
   }

   private static <K, V> EntryRetriever<K, V> blockEntryRetrieverResponse(final CheckPoint checkPoint, Cache<K, V> cache) {
      EntryRetriever<K, V> retriever = TestingUtil.extractComponent(cache, EntryRetriever.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(retriever);
      EntryRetriever mockRetriever = mock(EntryRetriever.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            checkPoint.trigger("pre_receive_response_invoked");
            checkPoint.await("pre_receive_response_released", 10, TimeUnit.SECONDS);
            try {
            return forwardedAnswer.answer(invocation);
            } finally {
               checkPoint.trigger("post_receive_response_invoked");
               checkPoint.await("post_receive_response_released", 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockRetriever).receiveResponse(any(UUID.class), any(Address.class), anySetOf(Integer.class),
                                             anySetOf(Integer.class), anyCollectionOf(CacheEntry.class));
      TestingUtil.replaceComponent(cache, EntryRetriever.class, mockRetriever, true);
      return retriever;
   }
}
