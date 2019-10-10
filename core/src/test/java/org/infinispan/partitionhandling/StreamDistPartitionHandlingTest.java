package org.infinispan.partitionhandling;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
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

   public void testUsingIteratorButPartitionOccursBeforeRetrievingRemoteValues() throws Exception {
      Cache<MagicKey, String> cache0 = cache(0);
      // Make sure we have 1 entry in each - since onEach will then be invoked once on each node
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      CheckPoint iteratorCP = new CheckPoint();
      // We let the completeable future be returned - but don't let it process the values yet
      iteratorCP.triggerForever(Mocks.BEFORE_RELEASE);
      // This must be before the stream is generated or else it won't see the update
      blockUntilRemoteNodesRespond(iteratorCP, cache0);
      try (CloseableIterator<?> iterator = Closeables.iterator(cache0.entrySet().stream())) {

         CheckPoint partitionCP = new CheckPoint();
         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(partitionCP, cache0);

         // We don't want to block the notifier
         partitionCP.triggerForever(Mocks.BEFORE_RELEASE);
         partitionCP.triggerForever(Mocks.AFTER_RELEASE);

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Wait until we have been notified before letting remote responses to arrive
         partitionCP.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

         // Afterwards let all the responses come in
         iteratorCP.triggerForever(Mocks.AFTER_RELEASE);

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

   public void testUsingIteratorButPartitionOccursAfterRetrievingRemoteValues() throws InterruptedException, TimeoutException, ExecutionException {
      Cache<MagicKey, String> cache0 = cache(0);
      // Make sure we have 1 entry in each - since onEach will then be invoked once on each node
      cache0.put(new MagicKey(cache(1), cache(2)), "not-local");
      cache0.put(new MagicKey(cache(0), cache(1)), "local");

      // Just retrieving the iterator will spawn the remote command
      try (CloseableIterator<?> iterator = Closeables.iterator(cache0.entrySet().stream())) {
         // Make sure we got one value
         assertTrue(iterator.hasNext());

         CheckPoint partitionCP = new CheckPoint();
         // Now we replace the notifier so we know when the notifier was told of the partition change so we know
         // our iterator should have been notified
         blockNotifierPartitionStatusChanged(partitionCP, cache0);

         // Now split the cluster
         splitCluster(new int[]{0, 1}, new int[]{2, 3});

         // Now let the notification occur after all the responses are done
         partitionCP.triggerForever(Mocks.BEFORE_RELEASE);
         partitionCP.triggerForever(Mocks.AFTER_RELEASE);

         // This should complete without issue now
         while (iterator.hasNext()) {
            iterator.next();
         }
      }
   }

   private static <K, V> void blockNotifierPartitionStatusChanged(final CheckPoint checkPoint,
                                                                                 Cache<K, V> cache) {
      Mocks.blockingMock(checkPoint, CacheNotifier.class, cache, (stub, m) ->
            stub.when(m).notifyPartitionStatusChanged(eq(AvailabilityMode.DEGRADED_MODE), eq(false)));
   }

   private static void blockUntilRemoteNodesRespond(final CheckPoint checkPoint, Cache<?, ?> cache) {
      RpcManager realManager = TestingUtil.extractComponent(cache, RpcManager.class);
      RpcManager spy = spy(realManager);

      doAnswer(invocation -> Mocks.blockingCompletableFuture(() -> {
               try {
                  return (CompletableFuture) invocation.callRealMethod();
               } catch (Throwable throwable) {
                  throw new AssertionError(throwable);
               }
            }, checkPoint).call()
      ).when(spy).invokeCommand(any(Address.class), any(InitialPublisherCommand.class), any(), any());

      TestingUtil.replaceComponent(cache, RpcManager.class, spy, true);
   }
}
