package org.infinispan.remoting.transport.jgroups;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.time.TimeService;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.jgroups.JChannel;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Base implementation of {@link JGroupsMetricsManager} that provides per-destination adaptive timeout tracking without
 * requiring a metrics registry.
 *
 * <p>
 * Always returns a functional {@link RequestTracker} whose {@code adjustTimeout()} reflects the destination's recent RPC
 * behaviour. Subclasses extend with extra functionalities.
 * </p>
 *
 * @since 16.3
 */
@Listener
@Scope(Scopes.GLOBAL)
public class BaseJGroupsMetricManager implements JGroupsMetricsManager, Lifecycle {

   private final Map<Address, DestinationState> destinations = new ConcurrentHashMap<>();

   @Inject TimeService timeService;
   @Inject CacheManagerNotifier notifier;

   @Start
   @Override
   public void start() {
      notifier.addListener(this);
   }

   @Stop
   @Override
   public void stop() {
      notifier.removeListener(this);
      destinations.values().forEach(DestinationState::onRemoved);
      destinations.clear();
   }

   @Override
   public RequestTracker trackRequest(Address destination, long flags) {
      DestinationState state = getOrCreateDestinationState(destination);
      return createRequestTracker(flags, state);
   }

   @Override
   public void recordMessageSent(Address destination, int bytesSent, boolean async) { }

   @Override
   public void onChannelConnected(JChannel channel, boolean isMainChannel) { }

   @Override
   public void onChannelDisconnected(JChannel channel) { }

   @Merged
   @ViewChanged
   public void onViewChanged(ViewChangedEvent event) {
      Set<Address> departed = new HashSet<>(destinations.keySet());
      event.getNewMembers().forEach(departed::remove);
      departed.forEach(address -> {
         DestinationState ds = destinations.remove(address);
         if (ds != null)
            ds.onRemoved();
      });
   }

   private RequestTracker createRequestTracker(long flags, DestinationState state) {
      return new RequestTrackerImpl(state, flags, timeService);
   }

   protected DestinationState getOrCreateDestinationState(Address address) {
      DestinationState state = destinations.get(address);
      if (state != null)
         return state;

      return destinations.computeIfAbsent(address, this::createDestinationState);
   }

   protected DestinationState createDestinationState(Address address) {
      return new DestinationState(address);
   }

   protected static class DestinationState {
      private final Address address;
      private final AdaptiveTimeout adaptiveTimeout = new AdaptiveTimeout();

      public DestinationState(Address address) {
         this.address = address;
      }

      public void recordSuccess(long durationNs) {
         adaptiveTimeout.recordSuccess(durationNs);
      }

      public void recordTimeout() {
         adaptiveTimeout.recordTimeout();
      }

      public Address address() {
         return address;
      }

      public AdaptiveTimeout adaptiveTimeout() {
         return adaptiveTimeout;
      }

      public void onRemoved() { }
   }

   static final class RequestTrackerImpl implements RequestTracker {
      private static final long STATE_TRANSFER_FLAGS = FlagBitSets.STATE_TRANSFER_PROGRESS
            | FlagBitSets.PUT_FOR_STATE_TRANSFER
            | FlagBitSets.PUT_FOR_X_SITE_STATE_TRANSFER;

      private final DestinationState state;
      private final long flags;
      private final TimeService timeService;

      private volatile long sentTimeNs;

      @GuardedBy("this")
      private volatile boolean completed;

      RequestTrackerImpl(DestinationState state, long flags, TimeService timeService) {
         this.state = state;
         this.flags = flags;
         this.timeService = timeService;
         this.sentTimeNs = timeService.time();
         state.adaptiveTimeout.incrementInFlight();
      }

      @Override
      public Address destination() {
         return state.address;
      }

      @Override
      public void resetSendTime() {
         synchronized (this) {
            if (completed) return;
            sentTimeNs = timeService.time();
         }
      }

      @Override
      public void resolve(Outcome outcome) {
         if (completed) return;
         synchronized (this) {
            if (completed) return;
            completed = true;
         }
         // Records the request outcome in the metrics.
         switch (outcome) {
            case SUCCESS -> {
               long duration = timeService.timeDuration(sentTimeNs, TimeUnit.NANOSECONDS);
               state.recordSuccess(duration);
            }
            case TIMEOUT -> state.recordTimeout();
            case ABANDONED -> {
               // Abandoned request doesn't update any values.
            }
         }
         state.adaptiveTimeout.removeInFlight();
      }

      @Override
      public long adjustTimeout(long timeoutNanos) {
         // Skip adjusting the timeout for operations in state transfer.
         if ((flags & STATE_TRANSFER_FLAGS) != 0)
            return timeoutNanos;
         return state.adaptiveTimeout.adjustTimeout(timeoutNanos);
      }

      @Override
      public boolean shouldShed() {
         if ((flags & STATE_TRANSFER_FLAGS) != 0)
            return false;
         return state.adaptiveTimeout.shouldShed();
      }
   }
}
