package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.metrics.Constants.JGROUPS_CLUSTER_TAG_NAME;
import static org.infinispan.metrics.Constants.JGROUPS_PREFIX;
import static org.infinispan.metrics.Constants.NODE_TAG_NAME;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.Constants;
import org.infinispan.metrics.impl.MetricUtils;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;

import net.jcip.annotations.GuardedBy;

/**
 * Concrete implementation of {@link JGroupsMetricsManager}.
 * <p>
 * It listens on view change to remove metrics for members that left the cluster.
 */
@Scope(Scopes.GLOBAL)
@Listener
public class JGroupsMetricsManagerImpl implements JGroupsMetricsManager {

   @Inject CacheManagerNotifier notifier;
   @Inject MetricsRegistry registry;
   @Inject TimeService timeService;

   private final Map<Address, DestinationMetrics> perDestinationMetrics;
   private final List<ClusterMetrics> otherChannels;
   private final boolean histogramEnabled;
   private volatile MainChannelRegistry mainChannelRegistry;
   private volatile boolean stopped = true;

   public JGroupsMetricsManagerImpl(boolean histogramEnabled) {
      this.histogramEnabled = histogramEnabled;
      perDestinationMetrics = new ConcurrentHashMap<>(16);
      otherChannels = new CopyOnWriteArrayList<>();
   }

   @Start
   public void start() {
      stopped = false;
      notifier.addListener(this);
   }

   @Stop
   public void stop() {
      stopped = true;
      notifier.removeListener(this);
      perDestinationMetrics.values().forEach(metrics -> metrics.unregister(registry));
      perDestinationMetrics.clear();
      otherChannels.forEach(metrics -> metrics.unregister(registry));
      mainChannelRegistry = null;
   }

   @ViewChanged
   @Merged
   public void onViewChanged(ViewChangedEvent event) {
      if (stopped) {
         return;
      }
      var leftMembers = new HashSet<>(perDestinationMetrics.keySet());
      event.getNewMembers().forEach(leftMembers::remove);

      for (Address node : leftMembers) {
         perDestinationMetrics.computeIfPresent(node, (unused, metrics) -> {
            metrics.unregister(registry);
            return null;
         });
      }
   }

   @Override
   public RequestTracker trackRequest(Address destination) {
      if (stopped) {
         return new NoOpRequestTracker(destination);
      }
      var metrics = get(destination);
      if (metrics == null) {
         return new NoOpRequestTracker(destination);
      }
      return new RequestTrackerImpl(destination, metrics, timeService);
   }

   @Override
   public void recordMessageSent(Address destination, int bytesSent, boolean async) {
      if (stopped) {
         return;
      }
      var metrics = get(destination);
      if (metrics == null) {
         return;
      }
      metrics.incrementBytesSent(bytesSent);
      if (async) {
         metrics.incrementAsyncRequests();
      }
   }

   @Override
   public synchronized void onChannelConnected(JChannel channel, boolean isMainChannel) {
      if (stopped) {
         return;
      }
      String nodeName = Objects.requireNonNull(nodeName(channel));
      String clusterName = Objects.requireNonNull(channel.clusterName());
      if (isMainChannel) {
         assert mainChannelRegistry == null;
         mainChannelRegistry = new MainChannelRegistry(nodeName, clusterName);
      }
      if (otherChannels.stream().map(m -> m.channel).noneMatch(ch -> ch.equals(channel))) {
         otherChannels.add(new ClusterMetrics(channel));
      }
      if (mainChannelRegistry != null) {
         otherChannels.forEach(clusterMetrics -> clusterMetrics.register(mainChannelRegistry));
      }
   }

   @Override
   public synchronized void onChannelDisconnected(JChannel channel) {
      if (stopped) {
         return;
      }
      if (mainChannelRegistry != null && mainChannelRegistry.clusterName.equals(channel.clusterName()) && mainChannelRegistry.nodeName.equals(channel.address().toString())) {
         mainChannelRegistry = null;
      }
      Optional<ClusterMetrics> optMetrics = otherChannels.stream().filter(m -> m.channel.equals(channel)).findFirst();
      if (optMetrics.isEmpty()) {
         return;
      }
      ClusterMetrics metrics = optMetrics.get();
      metrics.unregister(registry);
      otherChannels.remove(metrics);
   }

   private DestinationMetrics get(Address dst) {
      assert dst != null;
      return perDestinationMetrics.computeIfAbsent(dst, this::createDestinationMetrics);
   }

   private DestinationMetrics createDestinationMetrics(Address destination) {
      assert destination != null;
      var statsRegistry = mainChannelRegistry;
      if (statsRegistry == null) {
         return null;
      }

      var attributes = createAttributes(destination.toString());

      // DestinationMetricsBuilder stores the references for timers/counters/etc.
      DestinationMetricsBuilder builder = new DestinationMetricsBuilder();
      // registerMetrics sets all the fields
      var metricsIds = statsRegistry.registerStats(builder, attributes);
      // create DestinationMetrics
      return builder.build(metricsIds, histogramEnabled);
   }

   private static String nodeName(JChannel channel) {
      org.jgroups.Address addr = channel.address();
      return addr == null ? channel.name() : addr.toString();
   }

   private Collection<MetricInfo> createAttributes(String dst) {
      var tags = Map.of(Constants.TARGET_NODE, dst);
      List<MetricInfo> attrs = new ArrayList<>(4);
      attrs.add(MetricUtils.createCounter("AsyncRequests", "Number of asynchronous requests to " + dst,
            DestinationMetricsBuilder::setAsyncRequests, tags));
      attrs.add(MetricUtils.createCounter("TimedOutRequests", "Number of timed out requests to " + dst,
            DestinationMetricsBuilder::setTimedOutRequests, tags));

      if (histogramEnabled) {
         attrs.add(MetricUtils.createTimer("SyncRequests", "Number of synchronous requests to " + dst,
               DestinationMetricsBuilder::setSyncRequests, tags));
         attrs.add(MetricUtils.createDistributionSummary("BytesSent", "Bytes sent to " + dst,
               DestinationMetricsBuilder::setBytesSentSummary, tags));
      } else {
         attrs.add(MetricUtils.createFunctionTimer("SyncRequests", "Number of synchronous requests to " + dst,
               DestinationMetricsBuilder::setSyncRequests, TimeUnit.NANOSECONDS, tags));
         attrs.add(MetricUtils.createCounter("BytesSent", "Bytes sent to " + dst,
               DestinationMetricsBuilder::setBytesSentCounter, tags));
      }
      return attrs;
   }

   private static class DestinationMetricsBuilder {
      TimerTracker syncRequests;
      CounterTracker asyncRequests;
      CounterTracker timedOutRequests;
      DistributionSummaryTracker bytesSentSummary;
      CounterTracker bytesSentCounter;

      void setSyncRequests(TimerTracker syncRequests) {
         this.syncRequests = syncRequests;
      }

      void setAsyncRequests(CounterTracker asyncRequests) {
         this.asyncRequests = asyncRequests;
      }

      void setTimedOutRequests(CounterTracker timedOutRequests) {
         this.timedOutRequests = timedOutRequests;
      }

      void setBytesSentSummary(DistributionSummaryTracker bytesSentSummary) {
         this.bytesSentSummary = bytesSentSummary;
      }

      void setBytesSentCounter(CounterTracker bytesSentCounter) {
         this.bytesSentCounter = bytesSentCounter;
      }

      DestinationMetrics build(Set<Object> metricsIds, boolean histogramEnabled) {
         assert syncRequests != null;
         assert asyncRequests != null;
         assert timedOutRequests != null;

         IntConsumer bytesSentConsumer;
         if (histogramEnabled) {
            assert bytesSentSummary != null;
            bytesSentConsumer = bytesSentSummary::record;
         } else {
            assert bytesSentCounter != null;
            bytesSentConsumer = bytesSentCounter::increment;
         }

         return new DestinationMetrics(syncRequests, asyncRequests, timedOutRequests, bytesSentConsumer, metricsIds);
      }
   }

   private static class DestinationMetrics {
      final TimerTracker syncRequests;
      final CounterTracker asyncRequests;
      final CounterTracker timedOutRequests;
      final IntConsumer bytesSent;
      final Set<Object> metricsIds;

      DestinationMetrics(TimerTracker syncRequests, CounterTracker asyncRequests, CounterTracker timedOutRequests, IntConsumer bytesSent, Set<Object> metricsIds) {
         this.syncRequests = syncRequests;
         this.asyncRequests = asyncRequests;
         this.timedOutRequests = timedOutRequests;
         this.bytesSent = bytesSent;
         this.metricsIds = metricsIds;
      }

      void recordSyncMessage(long durationNanos) {
         syncRequests.update(Duration.ofNanos(durationNanos));
      }

      void incrementBytesSent(int size) {
         bytesSent.accept(size);
      }

      void incrementAsyncRequests() {
         asyncRequests.increment();
      }

      void incrementTimedOutRequests() {
         timedOutRequests.increment();
      }

      void unregister(MetricsRegistry registry) {
         registry.unregisterMetrics(metricsIds);
      }
   }

   private static class RequestTrackerImpl implements RequestTracker {
      private final Address destination;
      final DestinationMetrics metrics;
      final TimeService timeService;
      volatile long sentTimeNanos;
      @GuardedBy("this")
      boolean completed;

      RequestTrackerImpl(Address destination, DestinationMetrics metrics, TimeService timeService) {
         this.destination = destination;
         this.metrics = metrics;
         this.timeService = timeService;
         this.sentTimeNanos = timeService.time();
      }

      @Override
      public final Address destination() {
         return destination;
      }

      @Override
      public synchronized void resetSendTime() {
         if (completed) {
            return;
         }
         sentTimeNanos = timeService.time();
      }

      @Override
      public synchronized void onComplete() {
         if (completed) {
            return;
         }
         metrics.recordSyncMessage(timeService.timeDuration(sentTimeNanos, TimeUnit.NANOSECONDS));
         completed = true;
      }

      @Override
      public synchronized void onTimeout() {
         if (completed) {
            return;
         }
         metrics.incrementTimedOutRequests();
         completed = true;
      }
   }

   private static class ClusterMetrics {
      final JChannel channel;
      final Set<Object> metricsIds;
      @GuardedBy("this")
      private boolean registered;

      ClusterMetrics(JChannel channel) {
         this.channel = channel;
         metricsIds = new HashSet<>(32);
      }

      synchronized void register(MainChannelRegistry mainChannel) {
         if (registered) {
            return;
         }
         for (Protocol protocol : channel.getProtocolStack().getProtocols()) {
            Collection<MetricInfo> attributes = JGroupsMetricsMetadata.PROTOCOL_METADATA.get(protocol.getClass());
            if (attributes != null && !attributes.isEmpty()) {
               metricsIds.addAll(mainChannel.registerComponent(protocol, protocol.getName(), channel.clusterName(), attributes));
            }
         }
         registered = true;
      }

      synchronized void unregister(MetricsRegistry registry) {
         registry.unregisterMetrics(metricsIds);
         metricsIds.clear();
         registered = false;
      }
   }

   private class MainChannelRegistry {
      final String nodeName;
      final String clusterName;

      MainChannelRegistry(String nodeName, String clusterName) {
         this.nodeName = nodeName;
         this.clusterName = clusterName;
      }

      Set<Object> registerStats(Object instance, Collection<MetricInfo> attributes) {
         return registerComponent(instance, "stats", clusterName, attributes);
      }

      Set<Object> registerComponent(Object instance, String component, String clusterName, Collection<MetricInfo> attributes) {
         String prefix = registry.namesAsTags() ?
               JGROUPS_PREFIX + component.toLowerCase() + "_" :
               JGROUPS_PREFIX + clusterName + "_" + component.toLowerCase() + "_";
         return registry.registerMetrics(instance, attributes, prefix, Map.of(NODE_TAG_NAME, nodeName, JGROUPS_CLUSTER_TAG_NAME, clusterName));
      }
   }


}
