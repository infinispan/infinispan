package org.infinispan.conflict.impl;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionException;
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
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

/**
 * When several segment fetches fail concurrently during conflict detection — e.g. because the transfer source is
 * suspected or has crashed, as in {@code CrashedNodeDuringConflictResolutionTest} — each failing request completes
 * its future exceptionally and emits an onError signal into the pipeline's merge coordinator. The coordinator can
 * deliver at most one of them; any additional errors would be routed to RxJava's global undeliverable error handler.
 */
@Test(groups = "functional", testName = "conflict.impl.ConcurrentSegmentFetchFailureTest")
public class ConcurrentSegmentFetchFailureTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling()
            .whenSplit(PartitionHandling.ALLOW_READ_WRITES)
            .mergePolicy((preferredEntry, otherEntries) -> preferredEntry);
      // Both nodes own every segment so that each one has exactly one remote replica (node 1) to fetch state from.
      builder.clustering().hash().numOwners(2);
      createClusteredCaches(2, builder);
   }

   public void testFailingSegmentFetchesDoNotReportUndeliverables() {
      for (int i = 0; i < 10; i++)
         cache(0).put("key-" + i, "value-" + i);

      AtomicInteger stateRequests = new AtomicInteger();
      wrapInboundInvocationHandler(cache(1), handler -> new SuspectSegmentRequestHandler(handler, stateRequests));

      List<Throwable> undeliverable = Collections.synchronizedList(new ArrayList<>());
      Consumer<? super Throwable> previousHandler = RxJavaPlugins.getErrorHandler();
      RxJavaPlugins.setErrorHandler(undeliverable::add);

      CompletionStage<Void> stage;
      CacheTopology topology;
      try {
         AdvancedCache<Object, Object> cache = advancedCache(0);
         InternalConflictManager<Object, Object> cm = TestingUtil.extractComponent(cache, InternalConflictManager.class);
         topology = cache.getDistributionManager().getCacheTopology();

         // Every segment fetch from node 1 fails as soon as it is issued and several of them fail concurrently. The attempt
         // must still process every segment before completing exceptionally exactly once — through the returned stage —
         // without any error reaching RxJava's global handler or stopping early at the first failure.
         stage = cm.resolveConflicts(topology, new HashSet<>(topology.getMembers()));

         eventually(() -> stateRequests.get() >= 2);
      } finally {
         RxJavaPlugins.setErrorHandler(previousHandler);
      }

      Throwable error;
      try {
         // The attempt cannot succeed: every segment fetch fails. It may only report that failure once, through the stage.
         stage.toCompletableFuture().join();
         throw new AssertionError("The attempt must fail when every segment fetch fails");
      } catch (CompletionException e) {
         error = e.getCause();
      }

      assertNotNull(error);
      assertTrue(error instanceof SuspectException, "Expected a SuspectException but got: " + error);

      // Before #18295 was fixed the first failing fetch terminated this pipeline's merge coordinator immediately — cancelling
      // the segment enumeration and disposing every in-flight request — so only a handful of segments were ever attempted.
      // A failed attempt must now settle all its fetches before reporting once; asserting on the count is what makes this
      // regression observable even when no undeliverable error happens to slip through during a particular run.
      assertEquals(topology.getWriteConsistentHash().getNumSegments(), stateRequests.get());

      assertEquals(Collections.emptyList(), undeliverable, "Concurrent segment fetch failures must not be reported to RxJava's undeliverable error handler");
   }

   /**
    * Reports no bucket hashes so that every segment falls back to a full fetch through
    * {@link StateReceiverImpl#getAllReplicasForSegment}, then answers the resulting state requests as if it had been suspected.
    */
   private static class SuspectSegmentRequestHandler extends AbstractDelegatingHandler {
      final AtomicInteger stateRequests;

      SuspectSegmentRequestHandler(PerCacheInboundInvocationHandler delegate, AtomicInteger stateRequests) {
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
            reply.reply(CacheNotFoundResponse.INSTANCE);
            return;
         }
         delegate.handle(command, reply, order);
      }
   }
}
