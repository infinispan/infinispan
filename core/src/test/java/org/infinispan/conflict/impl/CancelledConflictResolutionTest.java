package org.infinispan.conflict.impl;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.conflict.GetBucketHashesCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

/**
 * Cancelling conflict resolution completes the outstanding {@link StateReceiverImpl} segment requests with a
 * {@link java.util.concurrent.CancellationException}. That must not be emitted into the conflict detection
 * pipeline, which is already being torn down at that point and can therefore only hand the exception to
 * RxJava's global undeliverable error handler.
 *
 * @see DefaultConflictManager#cancelConflictResolution()
 */
@Test(groups = "functional", testName = "conflict.impl.CancelledConflictResolutionTest")
public class CancelledConflictResolutionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling()
            .whenSplit(PartitionHandling.ALLOW_READ_WRITES)
            .mergePolicy((preferredEntry, otherEntries) -> preferredEntry);
      createClusteredCaches(2, builder);
   }

   public void testCancelWithInFlightSegmentRequests() {
      for (int i = 0; i < 10; i++)
         cache(0).put("key-" + i, "value-" + i);

      AtomicInteger stateRequests = new AtomicInteger();
      wrapInboundInvocationHandler(cache(1), handler -> new StallSegmentRequestHandler(handler, stateRequests));

      List<Throwable> undeliverable = Collections.synchronizedList(new ArrayList<>());
      Consumer<? super Throwable> previousHandler = RxJavaPlugins.getErrorHandler();
      RxJavaPlugins.setErrorHandler(undeliverable::add);
      try {
         AdvancedCache<Object, Object> cache = advancedCache(0);
         InternalConflictManager<Object, Object> cm = TestingUtil.extractComponent(cache, InternalConflictManager.class);
         CacheTopology topology = cache.getDistributionManager().getCacheTopology();
         CompletionStage<Void> stage = cm.resolveConflicts(topology, new HashSet<>(topology.getMembers()));

         // The segment requests only complete once node 1 replies, which it never does
         eventually(() -> stateRequests.get() > 0);
         cm.cancelConflictResolution();

         assertTrue(stage.toCompletableFuture().isDone(), "Cancelling must complete the returned stage");
      } finally {
         RxJavaPlugins.setErrorHandler(previousHandler);
      }

      assertEquals(Collections.emptyList(), undeliverable, "Cancelling conflict resolution must not report errors " +
            "to RxJava's undeliverable error handler");
   }

   /**
    * Reports no bucket hashes so that every segment falls back to a full fetch through
    * {@link StateReceiverImpl#getAllReplicasForSegment}, then never answers the resulting state requests.
    */
   private static class StallSegmentRequestHandler extends AbstractDelegatingHandler {
      final AtomicInteger stateRequests;

      StallSegmentRequestHandler(PerCacheInboundInvocationHandler delegate, AtomicInteger stateRequests) {
         super(delegate);
         this.stateRequests = stateRequests;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof GetBucketHashesCommand) {
            reply.reply(SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
            return;
         }
         if (command instanceof ConflictResolutionStartCommand) {
            stateRequests.incrementAndGet();
            return;
         }
         delegate.handle(command, reply, order);
      }
   }
}
