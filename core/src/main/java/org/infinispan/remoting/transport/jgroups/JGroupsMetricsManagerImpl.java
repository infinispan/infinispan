package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.metrics.Constants.JGROUPS_CLUSTER_TAG_NAME;
import static org.infinispan.metrics.Constants.JGROUPS_PREFIX;
import static org.infinispan.metrics.Constants.NODE_TAG_NAME;
import static org.infinispan.metrics.Constants.VENDOR_PREFIX;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntConsumer;

import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.Constants;
import org.infinispan.metrics.impl.MetricUtils;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.remoting.transport.Address;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Concrete implementation of {@link JGroupsMetricsManager}.
 * <p>
 * It listens on view change to remove metrics for members that left the cluster.
 */
@Scope(Scopes.GLOBAL)
public final class JGroupsMetricsManagerImpl extends BaseJGroupsMetricManager {

   @Inject MetricsRegistry registry;

   private final List<ClusterMetrics> otherChannels;
   private final boolean histogramEnabled;
   @Deprecated(forRemoval = true, since = "16.0")
   private String legacyGlobalPrefix;
   private volatile MainChannelRegistry mainChannelRegistry;
   private volatile boolean stopped = true;

   public JGroupsMetricsManagerImpl(boolean histogramEnabled, String legacyGlobalPrefix) {
      this.histogramEnabled = histogramEnabled;
      otherChannels = new CopyOnWriteArrayList<>();
      this.legacyGlobalPrefix = legacyGlobalPrefix;
   }

   @Start
   public void startInternal() {
      stopped = false;
   }

   @Stop
   public void stopInternal() {
      stopped = true;
      otherChannels.forEach(metrics -> metrics.unregister(registry));
      mainChannelRegistry = null;
   }

   @Override
   public void recordMessageSent(Address destination, int bytesSent, boolean async) {
      if (stopped) {
         return;
      }
      DestinationMetrics metrics = ((MetricsDestinationState) getOrCreateDestinationState(destination)).metrics();
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

   private DestinationMetrics createDestinationMetrics(Address destination, AdaptiveTimeout adaptiveTimeout) {
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

      metricsIds.addAll(statsRegistry.registerStats(adaptiveTimeout, adaptiveTimeoutGauges(destination.toString())));
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
               DestinationMetricsBuilder::setSyncRequests, tags));
         attrs.add(MetricUtils.createCounter("BytesSent", "Bytes sent to " + dst,
               DestinationMetricsBuilder::setBytesSentCounter, tags));
      }
      return attrs;
   }

   @Deprecated(forRemoval = true, since = "16.0")
   private String metricPrefix(String clusterName, String componentName) {
      var builder = new StringBuilder();
      if (legacyGlobalPrefix != null && !legacyGlobalPrefix.isEmpty()) {
         builder.append(legacyGlobalPrefix).append("_");
      }
      builder.append(JGROUPS_PREFIX);
      if (!registry.namesAsTags()) {
         builder.append(clusterName).append("_");
      }
      return builder.append(componentName).append("_").toString();
   }

   static Collection<MetricInfo> adaptiveTimeoutGauges(String dst) {
      Map<String, String> tags = Map.of(Constants.TARGET_NODE, dst);
      List<MetricInfo> attributes = new ArrayList<>();

      attributes.add(MetricUtils.createGauge("RoundTripTime",
            "Smoothed round-trip time to " + dst + " in nanoseconds", AdaptiveTimeout::srtt, tags));

      attributes.add(MetricUtils.createGauge("RoundTripTimeVariance",
            "Round-trip time variance to " + dst + " in nanoseconds", AdaptiveTimeout::rttvar, tags));

      attributes.add(MetricUtils.createGauge("TokenBucketLevel",
            "Adaptive timeout token bucket level for " + dst + " (full means healthy)", AdaptiveTimeout::bucketLevel, tags));

      attributes.add(MetricUtils.createGauge("InFlightRequests",
            "Number of in-flight requests to " + dst, AdaptiveTimeout::inFlight, tags));

      attributes.add(MetricUtils.createGauge("ConcurrencyLimit",
            "Adaptive concurrency limit for in-flight requests to " + dst, AdaptiveTimeout::concurrencyLimit, tags));

      return attributes;
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

   private record DestinationMetrics(TimerTracker syncRequests, CounterTracker asyncRequests,
                                     CounterTracker timedOutRequests, IntConsumer bytesSent, Set<Object> metricsIds) {

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

   @Override
   protected DestinationState createDestinationState(Address address) {
      return new MetricsDestinationState(address, this);
   }

   private static final class MetricsDestinationState extends DestinationState {
      private final JGroupsMetricsManagerImpl manager;
      private volatile DestinationMetrics metrics;

      public MetricsDestinationState(Address address, JGroupsMetricsManagerImpl manager) {
         super(address);
         this.manager = manager;
      }

      public DestinationMetrics metrics() {
         DestinationMetrics m = this.metrics;
         if (m != null)
            return m;

         synchronized (this) {
            if (this.metrics == null) {
               DestinationMetrics created = manager.createDestinationMetrics(address(), adaptiveTimeout());
               if (created != null) {
                  this.metrics = created;
               }
            }
            return this.metrics;
         }
      }

      @Override
      public void recordSuccess(long durationNs) {
         super.recordSuccess(durationNs);
         DestinationMetrics m = metrics();
         if (m != null)
            m.recordSyncMessage(durationNs);
      }

      @Override
      public void recordTimeout() {
         super.recordTimeout();
         DestinationMetrics m = metrics();
         if (m != null)
            m.incrementTimedOutRequests();
      }

      @Override
      public void onRemoved() {
         DestinationMetrics m = this.metrics;
         if (m != null)
            m.unregister(manager.registry);
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
         Map<String, String> tags = Map.of(NODE_TAG_NAME, nodeName, JGROUPS_CLUSTER_TAG_NAME, clusterName);
         if (registry.legacy()) {
            return registry.registerMetrics(instance, attributes, VENDOR_PREFIX + metricPrefix(clusterName, component.toLowerCase()), tags);
         } else {
            return registry.registerMetrics(instance, attributes, JGROUPS_PREFIX + component.toLowerCase() + "_", tags);
         }
      }
   }
}
