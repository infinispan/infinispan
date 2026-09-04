package org.infinispan.remoting.transport.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.AdaptiveTimeout;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManager;
import org.infinispan.remoting.transport.jgroups.RequestTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingleTargetRequestAdaptiveTimeoutTest {

   private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);

   @SuppressWarnings("unchecked")
   private final ScheduledFuture<Void> mockFuture = mock(ScheduledFuture.class);
   private ScheduledExecutorService executor;
   private JGroupsMetricsManager metricsManager;
   private RequestRepository repository;

   @BeforeEach
   void setUp() {
      executor = mock(ScheduledExecutorService.class);
      when(executor.schedule(any(Callable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockFuture);
      metricsManager = mock(JGroupsMetricsManager.class);
      repository = new RequestRepository(metricsManager, executor, null);
   }

   @Test
   void healthyTrackerUsesFullTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }

      Address target = Address.random("target");
      when(metricsManager.trackRequest(any(Address.class), anyLong()))
            .thenReturn(new TestRequestTracker(target, adaptive));

      SingleTargetRequest<Void> request = (SingleTargetRequest<Void>) repository.singleRequest(
            target, 0L, VoidResponseCollector.validOnly(), 15, TimeUnit.SECONDS);

      assertThat(request.getTimeoutMs()).isEqualTo(TimeUnit.SECONDS.toMillis(15));
   }

   @Test
   void degradedTrackerUsesReducedTimeout() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }
      for (int i = 0; i < 5; i++) {
         adaptive.recordTimeout();
      }

      Address target = Address.random("target");
      when(metricsManager.trackRequest(any(Address.class), anyLong()))
            .thenReturn(new TestRequestTracker(target, adaptive));

      SingleTargetRequest<Void> request = (SingleTargetRequest<Void>) repository.singleRequest(
            target, 0L, VoidResponseCollector.validOnly(), 15, TimeUnit.SECONDS);

      assertThat(request.getTimeoutMs()).isLessThan(TimeUnit.SECONDS.toMillis(15));
      assertThat(request.getTimeoutMs()).isGreaterThan(0);
   }

   @Test
   void partiallyDegradedTrackerInterpolates() {
      AdaptiveTimeout adaptive = new AdaptiveTimeout();
      for (int i = 0; i < 20; i++) {
         adaptive.recordSuccess(10 * MS);
      }
      adaptive.recordTimeout();
      adaptive.recordTimeout();

      Address target = Address.random("target");
      when(metricsManager.trackRequest(any(Address.class), anyLong()))
            .thenReturn(new TestRequestTracker(target, adaptive));

      SingleTargetRequest<Void> request = (SingleTargetRequest<Void>) repository.singleRequest(
            target, 0L, VoidResponseCollector.validOnly(), 15, TimeUnit.SECONDS);

      assertThat(request.getTimeoutMs()).isLessThan(TimeUnit.SECONDS.toMillis(15));
      assertThat(request.getTimeoutMs()).isGreaterThan(TimeUnit.MILLISECONDS.toMillis(10));
   }

   @Test
   void multiTargetEarlyCompletionReleasesAbandonedTrackers() {
      Address responder = Address.random("responder");
      Address abandoned = Address.random("abandoned");

      RequestTracker responderTracker = mock(RequestTracker.class);
      when(responderTracker.destination()).thenReturn(responder);
      RequestTracker abandonedTracker = mock(RequestTracker.class);
      when(abandonedTracker.destination()).thenReturn(abandoned);

      when(metricsManager.trackRequest(responder, 0L)).thenReturn(responderTracker);
      when(metricsManager.trackRequest(abandoned, 0L)).thenReturn(abandonedTracker);

      // validOnly() finishes on the first valid response, so the request completes while `abandoned` is still outstanding.
      Request<Address, ?> request = repository.multiRequest(
            List.of(responder, abandoned), null, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), responder, SUCCESSFUL_EMPTY_RESPONSE);

      verify(responderTracker).resolve(RequestTracker.Outcome.SUCCESS);
      verify(abandonedTracker).resolve(RequestTracker.Outcome.ABANDONED);
      verify(abandonedTracker, never()).resolve(RequestTracker.Outcome.SUCCESS);
      verify(abandonedTracker, never()).resolve(RequestTracker.Outcome.TIMEOUT);
   }

   @Test
   void cancelledSingleTargetRequestReleasesTracker() {
      Address target = Address.random("target");

      RequestTracker tracker = mock(RequestTracker.class);
      when(tracker.destination()).thenReturn(target);
      when(metricsManager.trackRequest(target, 0L)).thenReturn(tracker);

      Request<Address, ?> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      // The request is abandoned without a response, as happens when the cache manager shuts down.
      request.toCompletableFuture().cancel(true);

      verify(tracker).resolve(RequestTracker.Outcome.ABANDONED);
      verify(tracker, never()).resolve(RequestTracker.Outcome.SUCCESS);
      verify(tracker, never()).resolve(RequestTracker.Outcome.TIMEOUT);
   }

   @Test
   void singleTargetLeaverAbandonsTrackerWithoutRecording() {
      Address target = Address.random("target");

      RequestTracker tracker = mock(RequestTracker.class);
      when(tracker.destination()).thenReturn(target);
      when(metricsManager.trackRequest(target, 0L)).thenReturn(tracker);

      SingleTargetRequest<?> request = (SingleTargetRequest<?>) repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      // The target leaves the cluster before replying: a leaver is neither a success nor a timeout.
      request.onNewView(Set.of(Address.random("other")));

      verify(tracker).resolve(RequestTracker.Outcome.ABANDONED);
      verify(tracker, never()).resolve(RequestTracker.Outcome.SUCCESS);
      verify(tracker, never()).resolve(RequestTracker.Outcome.TIMEOUT);
   }

   private record TestRequestTracker(Address destination, AdaptiveTimeout adaptiveTimeout) implements RequestTracker {

      @Override
      public void resetSendTime() { }

      @Override
      public void resolve(Outcome outcome) { }

      @Override
      public long adjustTimeout(long timeoutNanos) {
         return adaptiveTimeout.adjustTimeout(timeoutNanos);
      }

      @Override
      public boolean shouldShed() {
         return false;
      }
   }
}
