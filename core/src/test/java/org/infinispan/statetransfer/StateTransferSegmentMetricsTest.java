package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "statetransfer.StateTransferSegmentMetricsTest")
@CleanupAfterMethod
public class StateTransferSegmentMetricsTest extends BaseDistFunctionalTest<String, String> {

   private final int MAX_NUM_SEGMENTS = 6;
   private final int[][] owners1And2 = new int[][]{{0, 1}, {0, 1}, {0, 1}, {0, 1}, {0, 1}, {0, 1}};
   private final int[][] owners1And3 = new int[][]{{0, 2}, {0, 2}, {0, 2}, {0, 2}, {0, 2}, {0, 2}};
   private final ControlledConsistentHashFactory factory = new ControlledConsistentHashFactory.Default(owners1And2);

   public StateTransferSegmentMetricsTest() {
      INIT_CLUSTER_SIZE = 3;
      numOwners = 2;
      performRehashing = true;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   {
      transactional = true;
   }

   @AfterMethod
   public void resetFactory() {
      factory.setOwnerIndexes(owners1And2);
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.clustering().hash().consistentHashFactory(factory).numSegments(MAX_NUM_SEGMENTS)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.OPTIMISTIC);
      return builder;
   }

   @Test
   public void testSegmentCounterDuringStateTransfer() throws Exception {
      final StateTransferManager manager = TestingUtil.extractComponent(c3, StateTransferManager.class);
      final CheckPoint checkPoint = new CheckPoint();

      // Node 3 will request node 1 for transactions
      waitTransactionRequest(c1, checkPoint);

      // We have to wait until non owner has the new topology installed before transferring state
      waitRequestingSegments(c3, checkPoint);

      // Wait to receive segment batch.
      waitApplyingSegmentBatch(c3, checkPoint);

      // Change the ownership.
      factory.setOwnerIndexes(owners1And3);

      // New node joins the cluster to trigger the topology change.
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(getSerializationContext());
      cm.defineConfiguration(cacheName, configuration.build());
      Future<Void> join = fork(() -> {
         waitForClusterToForm(cacheName);
         log.debug("4th has joined");
         return null;
      });

      // Node 3 is notified about topology change and request all the segments since the ownership changed.
      // This also involves requesting the transactions, we wait to the provider receive the request
      checkPoint.awaitStrict("topology_update_notify_invoked_" + c3, 10, TimeUnit.SECONDS);
      checkPoint.awaitStrict("transactions_requested_invoked_" + c1, 10, TimeUnit.SECONDS);

      assertEquals(manager.getInflightTransactionalSegmentCount(), MAX_NUM_SEGMENTS);
      // Transferring states should be in progress.
      assertTrue(manager.isStateTransferInProgress());

      checkPoint.triggerForever("transactions_requested_released_" + c1);
      checkPoint.awaitStrict("topology_update_notify_executed_" + c3, 10, TimeUnit.SECONDS);

      // Node 3 already received the transactional segments
      assertEquals(manager.getInflightTransactionalSegmentCount(), 0);

      // Node 3 ask for data from all the segments he currently owns.
      assertEquals(manager.getInflightSegmentTransferCount(), MAX_NUM_SEGMENTS);

      checkPoint.triggerForever("topology_update_notify_released_" + c3);

      checkPoint.awaitStrict("state_installed_invoked_" + c3, 10, TimeUnit.SECONDS);
      assertEquals(manager.getInflightSegmentTransferCount(), MAX_NUM_SEGMENTS);
      checkPoint.triggerForever("state_installed_invoked_release_" + c3);

      // Wait until the batch is applied. If, for any reason, the batch does not have all the segments this will fail.
      checkPoint.awaitStrict("state_applied_" + c3, 10, TimeUnit.SECONDS);
      assertEquals(manager.getInflightSegmentTransferCount(), 0);

      // We do not actually care about the new node.
      join.cancel(true);
   }

   private void waitTransactionRequest(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateProvider sp = TestingUtil.extractComponent(cache, StateProvider.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sp);
      StateProvider mockProvider = mock(StateProvider.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         checkPoint.trigger("transactions_requested_invoked_" + cache);
         Object response = forwardedAnswer.answer(invocation);

         try {
            checkPoint.awaitStrict("transactions_requested_released_" + cache, 10, TimeUnit.SECONDS);
            return response;
         } catch (InterruptedException | TimeoutException e) {
            throw new TestException(e);
         }
      }).when(mockProvider).getTransactionsForSegments(any(Address.class), anyInt(), any(IntSet.class));
      TestingUtil.replaceComponent(cache, StateProvider.class, mockProvider, true);
   }

   private void waitRequestingSegments(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateConsumer sc = TestingUtil.extractComponent(cache, StateConsumer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sc);
      StateConsumer mockConsumer = mock(StateConsumer.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("topology_update_notify_invoked_" + cache);
         return ((CompletionStage<?>) forwardedAnswer.answer(invocation)).thenRun(() -> {
            // Now wait until main thread lets us through
            checkPoint.trigger("topology_update_notify_executed_" + cache);
            try {
               checkPoint.awaitStrict("topology_update_notify_released_" + cache, 10, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
               Thread.currentThread().interrupt();
            }
         });
      }).when(mockConsumer).onTopologyUpdate(any(CacheTopology.class), anyBoolean());
      TestingUtil.replaceComponent(cache, StateConsumer.class, mockConsumer, true);
   }

   private void waitApplyingSegmentBatch(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateConsumer sc = TestingUtil.extractComponent(cache, StateConsumer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sc);
      StateConsumer mockConsumer = mock(StateConsumer.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Sync with main thread
         checkPoint.trigger("state_installed_invoked_" + cache);
         // Proceed when main thread allows
         checkPoint.awaitStrict("state_installed_invoked_release_" + cache, 10, TimeUnit.SECONDS);

         // Apply the whole batch of segments and then issue a signal back to main thread.
         return ((CompletionStage<?>) forwardedAnswer.answer(invocation))
               .thenRun(() -> checkPoint.trigger("state_applied_" + cache));
      }).when(mockConsumer).applyState(any(Address.class), anyInt(), anyCollection());
      TestingUtil.replaceComponent(cache, StateConsumer.class, mockConsumer, true);
   }
}
