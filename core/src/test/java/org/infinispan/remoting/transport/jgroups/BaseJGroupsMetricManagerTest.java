package org.infinispan.remoting.transport.jgroups;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.remoting.transport.Address;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BaseJGroupsMetricManagerTest {

   private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);
   private static final long CONFIGURED_TIMEOUT = TimeUnit.SECONDS.toNanos(15);

   private ControlledTimeService timeService;
   private BaseJGroupsMetricManager manager;

   @BeforeEach
   void setUp() {
      timeService = new ControlledTimeService();
      manager = new BaseJGroupsMetricManager();
      manager.timeService = timeService;
   }

   @Test
   void trackRequestReturnsTrackerWithFunctionalAdaptiveTimeout() {
      Address target = Address.random("target");

      // Train: 20 successful RPCs at 10ms latency
      for (int i = 0; i < 20; i++) {
         RequestTracker tracker = manager.trackRequest(target, 0L);
         timeService.advance(10, TimeUnit.MILLISECONDS);
         tracker.resolve(RequestTracker.Outcome.SUCCESS);
      }

      // Drain: 5 consecutive timeouts
      for (int i = 0; i < 5; i++) {
         RequestTracker tracker = manager.trackRequest(target, 0L);
         tracker.resolve(RequestTracker.Outcome.TIMEOUT);
      }

      // Fresh trackers for the same destination should have shortened timeout.
      // One call may be a probe (returns configured timeout), so we allow at most one.
      int probeCount = 0;
      for (int i = 0; i < 3; i++) {
         RequestTracker tracker = manager.trackRequest(target, 0L);
         long adjusted = tracker.adjustTimeout(CONFIGURED_TIMEOUT);

         assertThat(adjusted)
               .as("timeout must remain positive")
               .isGreaterThan(0);

         if (adjusted == CONFIGURED_TIMEOUT) {
            probeCount++;
         } else {
            assertThat(adjusted)
                  .as("non-probe call should return shortened timeout")
                  .isLessThan(CONFIGURED_TIMEOUT);
         }
      }

      assertThat(probeCount)
            .as("at most one probe among three calls")
            .isLessThanOrEqualTo(1);
   }

   @Test
   void inFlightRequestsShortenTimeout() {
      Address target = Address.random("target");

      // Train SRTT to ~10ms. Each request starts and completes, so in-flight returns to zero.
      for (int i = 0; i < 20; i++) {
         RequestTracker tracker = manager.trackRequest(target, 0L);
         timeService.advance(10, TimeUnit.MILLISECONDS);
         tracker.resolve(RequestTracker.Outcome.SUCCESS);
      }

      // Healthy baseline: full bucket, effectively no congestion.
      RequestTracker baseline = manager.trackRequest(target, 0L);
      long baselineTimeout = baseline.adjustTimeout(CONFIGURED_TIMEOUT);
      baseline.resolve(RequestTracker.Outcome.SUCCESS);

      // Pile up: 200 requests start but none complete, so in-flight stays high.
      for (int i = 0; i < 200; i++) {
         manager.trackRequest(target, 0L);
      }

      RequestTracker congested = manager.trackRequest(target, 0L);
      long congestedTimeout = congested.adjustTimeout(CONFIGURED_TIMEOUT);

      assertThat(congestedTimeout)
            .as("piled-up in-flight requests should shorten the timeout for the destination")
            .isLessThan(baselineTimeout);
   }

   @Test
   void releasedRequestsFreeInFlightWithoutDegradingDestination() {
      Address target = Address.random("target");

      // Train to a healthy, full-bucket state; in-flight returns to zero.
      for (int i = 0; i < 20; i++) {
         RequestTracker tracker = manager.trackRequest(target, 0L);
         timeService.advance(10, TimeUnit.MILLISECONDS);
         tracker.resolve(RequestTracker.Outcome.SUCCESS);
      }

      // A batch piles up and is then abandoned (e.g. multi-target early completion, or cancellation).
      RequestTracker[] abandoned = new RequestTracker[200];
      for (int i = 0; i < 200; i++) {
         abandoned[i] = manager.trackRequest(target, 0L);
      }
      for (RequestTracker tracker : abandoned) {
         tracker.resolve(RequestTracker.Outcome.ABANDONED);
      }

      // In-flight is back to zero and no timeout was recorded, so the bucket is still full:
      // a fresh request sees the full configured timeout again.
      RequestTracker after = manager.trackRequest(target, 0L);
      assertThat(after.adjustTimeout(CONFIGURED_TIMEOUT))
            .as("released requests free their in-flight slot without degrading the destination")
            .isEqualTo(CONFIGURED_TIMEOUT);
   }

   @Test
   void stateTransferRequestsAreNeverShed() {
      Address target = Address.random("target");

      // Degrade the destination: consecutive timeouts shrink its adaptive concurrency limit.
      for (int i = 0; i < 4; i++) {
         manager.trackRequest(target, 0L).resolve(RequestTracker.Outcome.TIMEOUT);
      }

      // Pile in-flight for this destination past the shrunken limit.
      for (int i = 0; i < 20_000; i++) {
         manager.trackRequest(target, 0L);
      }

      RequestTracker regular = manager.trackRequest(target, 0L);
      assertThat(regular.shouldShed())
            .as("a severely backlogged destination should shed ordinary requests")
            .isTrue();

      RequestTracker stateTransfer = manager.trackRequest(target, FlagBitSets.STATE_TRANSFER_PROGRESS);
      assertThat(stateTransfer.shouldShed())
            .as("state transfer must never be shed, even when the destination is severely backlogged")
            .isFalse();
   }

   @Test
   void viewChangeRemovesDepartedMemberState() {
      Address nodeA = Address.random("nodeA");
      Address nodeB = Address.random("nodeB");
      Address self = Address.random("self");

      // Train both nodes with successful RPCs
      for (int i = 0; i < 20; i++) {
         RequestTracker trackerA = manager.trackRequest(nodeA, 0L);
         RequestTracker trackerB = manager.trackRequest(nodeB, 0L);
         timeService.advance(10, TimeUnit.MILLISECONDS);
         trackerA.resolve(RequestTracker.Outcome.SUCCESS);
         trackerB.resolve(RequestTracker.Outcome.SUCCESS);
      }

      // Degrade nodeA: drain its bucket
      for (int i = 0; i < 5; i++) {
         manager.trackRequest(nodeA, 0L).resolve(RequestTracker.Outcome.TIMEOUT);
      }

      // nodeA leaves the cluster, only self and nodeB remain
      EventImpl viewChange = new EventImpl();
      viewChange.setNewMembers(List.of(self, nodeB));
      manager.onViewChanged(viewChange);

      // nodeA "rejoins", should get fresh state, not the stale drained bucket.
      // A fresh AdaptiveTimeout has no samples, so adjustTimeout returns configured timeout.
      RequestTracker freshTracker = manager.trackRequest(nodeA, 0L);
      assertThat(freshTracker.adjustTimeout(CONFIGURED_TIMEOUT))
            .as("rejoined node should get full configured timeout (fresh state)")
            .isEqualTo(CONFIGURED_TIMEOUT);

      // nodeB was not removed, its trained state should still be present.
      // Full bucket + SRTT initialised, should still return configured timeout.
      RequestTracker trackerB = manager.trackRequest(nodeB, 0L);
      assertThat(trackerB.adjustTimeout(CONFIGURED_TIMEOUT))
            .as("surviving node keeps its trained state")
            .isEqualTo(CONFIGURED_TIMEOUT);
   }
}
