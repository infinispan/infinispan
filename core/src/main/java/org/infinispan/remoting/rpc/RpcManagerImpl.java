package org.infinispan.remoting.rpc;

import static org.infinispan.remoting.rpc.RpcManagerImpl.OBJECT_NAME;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeListener;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.logging.TraceException;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.metrics.Constants;
import org.infinispan.metrics.impl.CustomMetricsSupplier;
import org.infinispan.metrics.impl.MetricUtils;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.CacheSpanAttribute;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteCacheRequest;
import org.infinispan.xsite.metrics.XSiteMetricsCollector;

/**
 * This component really is just a wrapper around a {@link org.infinispan.remoting.transport.Transport} implementation,
 * and is used to set up the transport and provide lifecycle and dependency hooks into external transport
 * implementations.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 4.0
 */
@MBean(objectName = OBJECT_NAME, description = "Manages all remote calls to remote cache instances in the cluster.")
@Scope(Scopes.NAMED_CACHE)
public class RpcManagerImpl implements RpcManager, JmxStatisticsExposer, CustomMetricsSupplier {

   public static final String OBJECT_NAME = "RpcManager";
   private static final Log log = LogFactory.getLog(RpcManagerImpl.class);

   @Inject Transport t;
   @Inject Configuration configuration;
   @Inject ComponentRef<CommandsFactory> cf;
   @Inject DistributionManager distributionManager;
   @Inject TimeService timeService;
   @Inject XSiteMetricsCollector xSiteMetricsCollector;

   private final Function<CacheRpcCommand, CacheRpcCommand> setClusterTraceSpanAttributes = this::initRpcCommand;
   private final AttributeListener<TimeQuantity> updateRpcOptions = this::updateRpcOptions;
   private final XSiteResponse.XSiteResponseCompleted xSiteResponseCompleted = this::registerXSiteTime;

   private final LongAdder replicationCount = new LongAdder();
   private final LongAdder replicationFailures = new LongAdder();
   private final LongAdder totalReplicationTime = new LongAdder();

   private boolean statisticsEnabled = false; // by default, don't gather statistics.

   private volatile RpcOptions syncRpcOptions;
   private InfinispanSpanAttributes clusterSpanAttributes;
   private InfinispanSpanAttributes xSiteSpanAttributes;

   @Override
   public Collection<MetricInfo> getCustomMetrics(GlobalMetricsConfiguration configuration) {
      var histograms = configuration.histograms();
      List<MetricInfo> attributes = new ArrayList<>(7);

      // global metric
      if (histograms) {
         attributes.add(MetricUtils.<RpcManagerImpl>createTimer("CrossSiteReplicationTimes",
               "Cross Site Replication Times",
               (rpcManager, timerTracker) -> rpcManager.xSiteMetricsCollector.registerTimer(timerTracker), null));
      } else {
         attributes.add(MetricUtils.<RpcManagerImpl>createFunctionTimer("CrossSiteReplicationTimes",
               "Cross Site Replication Times",
               (rpcManager, timerTracker) -> rpcManager.xSiteMetricsCollector.registerTimer(timerTracker),
               null));
      }

      for (String site : xSiteMetricsCollector.sites()) {
         Map<String, String> tags = Map.of(Constants.SITE_TAG_NAME, site);
         if (configuration.legacy() && !configuration.namesAsTags()) {
            String lSite = site.toLowerCase();
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("AverageXSiteReplicationTimeTo_" + lSite,
                  "Average Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getAverageXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("MinimumXSiteReplicationTimeTo_" + lSite,
                  "Minimum Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getMinimumXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("MaximumXSiteReplicationTimeTo_" + lSite,
                  "Maximum Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getMaximumXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("NumberXSiteRequestsSentTo_" + lSite,
                  "Number of Cross-Site request sent to " + site,
                  rpcManager -> rpcManager.getNumberXSiteRequestsSentTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("NumberXSiteRequestsReceivedFrom_" + lSite,
                  "Number of Cross-Site request received from " + site,
                  rpcManager -> rpcManager.getNumberXSiteRequestsReceivedFrom(site), tags));
            if (histograms) {
               attributes.add(MetricUtils.<RpcManagerImpl>createTimer("ReplicationTimesTo_" + lSite,
                     "Replication times to " + site,
                     (rpcManager, timer) -> rpcManager.xSiteMetricsCollector.registerTimer(site, timer), tags));
            } else {
               attributes.add(MetricUtils.<RpcManagerImpl>createFunctionTimer("ReplicationTimesTo_" + lSite,
                     "Replication times to " + site,
                     (rpcManager, timer) -> rpcManager.xSiteMetricsCollector.registerTimer(site, timer),
                     tags));
            }
         } else {
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("AverageXSiteReplicationTimeToSite",
                  "Average Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getAverageXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("MinimumXSiteReplicationTimeToSite",
                  "Minimum Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getMinimumXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("MaximumXSiteReplicationTimeToSite",
                  "Maximum Cross-Site replication time to " + site,
                  rpcManager -> rpcManager.getMaximumXSiteReplicationTimeTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("NumberXSiteRequestsSentToSite",
                  "Number of Cross-Site request sent to " + site,
                  rpcManager -> rpcManager.getNumberXSiteRequestsSentTo(site), tags));
            attributes.add(MetricUtils.<RpcManagerImpl>createGauge("NumberXSiteRequestsReceivedFromSite",
                  "Number of Cross-Site request received from " + site,
                  rpcManager -> rpcManager.getNumberXSiteRequestsReceivedFrom(site), tags));
            if (histograms) {
               attributes.add(MetricUtils.<RpcManagerImpl>createTimer("ReplicationTimesToSite",
                     "Replication times to " + site,
                     (rpcManager, timer) -> rpcManager.xSiteMetricsCollector.registerTimer(site, timer), tags));
            } else {
               attributes.add(MetricUtils.<RpcManagerImpl>createFunctionTimer("ReplicationTimesToSite",
                     "Replication times to " + site,
                     (rpcManager, timer) -> rpcManager.xSiteMetricsCollector.registerTimer(site, timer),
                     tags));
            }
         }
      }
      return attributes;
   }

   @Start
   void start() {
      statisticsEnabled = configuration.statistics().enabled();

      configuration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener(updateRpcOptions);
      updateRpcOptions(configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT), null);
   }

   @Stop
   void stop() {
      configuration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .removeListener(updateRpcOptions);
   }

   @Inject
   void cacheSpanAttributes(CacheSpanAttribute cacheSpanAttribute) {
      clusterSpanAttributes = cacheSpanAttribute.getAttributes(SpanCategory.CLUSTER);
      xSiteSpanAttributes = cacheSpanAttribute.getAttributes(SpanCategory.X_SITE);
   }

   private void updateRpcOptions(Attribute<TimeQuantity> attribute, TimeQuantity oldValue) {
      syncRpcOptions = new RpcOptions(DeliverOrder.NONE, attribute.get().longValue(), TimeUnit.MILLISECONDS);
   }

   @ManagedAttribute(description = "Retrieves the committed view.", displayName = "Committed view", dataType = DataType.TRAIT)
   public String getCommittedViewAsString() {
      CacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (cacheTopology == null)
         return "N/A";

      return cacheTopology.getCurrentCH().getMembers().toString();
   }

   @ManagedAttribute(description = "Retrieves the pending view.", displayName = "Pending view", dataType = DataType.TRAIT)
   public String getPendingViewAsString() {
      CacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (cacheTopology == null)
         return "N/A";

      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      return pendingCH != null ? pendingCH.getMembers().toString() : "null";
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, CacheRpcCommand command,
                                               ResponseCollector<Address, T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      if (!statisticsEnabled) {
         return t.invokeCommand(target, cacheRpc, collector, rpcOptions.deliverOrder(),
                                rpcOptions.timeout(), rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommand(target, cacheRpc, collector, rpcOptions.deliverOrder(),
                                      rpcOptions.timeout(), rpcOptions.timeUnit());
      } catch (Exception e) {
         return errorReplicating(e);
      }
      return invocation.handle((response, throwable) -> updateStatistics(startTimeNanos, response, throwable));
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, CacheRpcCommand command,
                                               ResponseCollector<Address, T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      if (!statisticsEnabled) {
         return t.invokeCommand(targets, cacheRpc, collector, rpcOptions.deliverOrder(),
                                rpcOptions.timeout(), rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommand(targets, cacheRpc, collector, rpcOptions.deliverOrder(),
                                      rpcOptions.timeout(), rpcOptions.timeUnit());
      } catch (Exception e) {
         return errorReplicating(e);
      }
      return invocation.handle((response, throwable) -> updateStatistics(startTimeNanos, response, throwable));
   }

   private <T> T updateStatistics(long startTimeNanos, T response, Throwable throwable) {
      long timeTaken = timeService.timeDuration(startTimeNanos, TimeUnit.MILLISECONDS);
      totalReplicationTime.add(timeTaken);

      if (throwable == null) {
         if (statisticsEnabled) {
            replicationCount.increment();
         }
         return response;
      } else {
         if (statisticsEnabled) {
            replicationFailures.increment();
         }
         return rethrowAsCacheException(throwable);
      }
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(CacheRpcCommand command, ResponseCollector<Address, T> collector,
                                                    RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = initRpcCommand(command);
      List<Address> cacheMembers = distributionManager.getCacheTopology().getMembers();

      if (!statisticsEnabled) {
         return t.invokeCommandOnAll(cacheMembers, cacheRpc, collector, rpcOptions.deliverOrder(),
                                     rpcOptions.timeout(), rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommandOnAll(cacheMembers, cacheRpc, collector, rpcOptions.deliverOrder(),
                                           rpcOptions.timeout(), rpcOptions.timeUnit());
      } catch (Exception e) {
         return errorReplicating(e);
      }
      return invocation.handle((response, throwable) -> updateStatistics(startTimeNanos, response, throwable));
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, CacheRpcCommand command,
                                                        ResponseCollector<Address, T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      if (!statisticsEnabled) {
         return t.invokeCommandStaggered(targets, cacheRpc, collector, rpcOptions.deliverOrder(), rpcOptions.timeout(),
                                         rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommandStaggered(targets, cacheRpc, collector, rpcOptions.deliverOrder(),
                                               rpcOptions.timeout(), rpcOptions.timeUnit());
      } catch (Exception e) {
         return errorReplicating(e);
      }
      return invocation.handle((response, throwable) -> updateStatistics(startTimeNanos, response, throwable));
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, CacheRpcCommand> commandGenerator,
                                                ResponseCollector<Address, T> collector, RpcOptions rpcOptions) {
      if (!statisticsEnabled) {
         return t.invokeCommands(targets, commandGenerator.andThen(setClusterTraceSpanAttributes), collector,
                                 rpcOptions.deliverOrder(), rpcOptions.timeout(), rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommands(targets, commandGenerator.andThen(setClusterTraceSpanAttributes), collector,
                                       rpcOptions.deliverOrder(), rpcOptions.timeout(), rpcOptions.timeUnit());
      } catch (Exception e) {
         return errorReplicating(e);
      }
      return invocation.handle((response, throwable) -> updateStatistics(startTimeNanos, response, throwable));
   }

   @Override
   public <T> T blocking(CompletionStage<T> request) {
      try {
         return CompletableFutures.await(request.toCompletableFuture());
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException("Thread interrupted while invoking RPC", e);
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         cause.addSuppressed(new TraceException());
         if (cause instanceof CacheException) {
            throw ((CacheException) cause);
         } else {
            throw new CacheException("Unexpected exception replicating command", cause);
         }
      }
   }

   private <T> T rethrowAsCacheException(Throwable throwable) {
      if (throwable.getCause() != null && throwable instanceof CompletionException) {
         throwable = throwable.getCause();
      }
      if (throwable instanceof CacheException) {
         log.trace("Replication exception", throwable);
         throw ((CacheException) throwable);
      } else {
         CLUSTER.unexpectedErrorReplicating(throwable);
         throw new CacheException(throwable);
      }
   }

   @Override
   public void sendTo(Address destination, CacheRpcCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      try {
         t.sendTo(destination, cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   @Override
   public void sendToMany(Collection<Address> destinations, CacheRpcCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      try {
         t.sendToMany(destinations, cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   @Override
   public void sendToAll(CacheRpcCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = initRpcCommand(command);

      try {
         t.sendToAll(cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   @Override
   public <O> XSiteResponse<O> invokeXSite(XSiteBackup backup, XSiteCacheRequest<O> command) {
      command.setSpanAttributes(xSiteSpanAttributes);
      if (!statisticsEnabled) {
         return t.backupRemotely(backup, command);
      }
      XSiteResponse<O> rsp = t.backupRemotely(backup, command);
      rsp.whenCompleted(xSiteResponseCompleted);
      return rsp;
   }

   private void registerXSiteTime(XSiteBackup backup, long sentTimestamp, long durationNanos, Throwable ignored) {
      if (durationNanos <= 0) {
         // no network involved
         return;
      }
      xSiteMetricsCollector.recordRequestSent(backup.getSiteName(), durationNanos, TimeUnit.NANOSECONDS);
   }

   private <T> T errorReplicating(Throwable t) {
      CLUSTER.unexpectedErrorReplicating(t);
      if (statisticsEnabled) {
         replicationFailures.increment();
      }
      return rethrowAsCacheException(t);
   }

   @Override
   public Transport getTransport() {
      return t;
   }

   private void setTopologyId(CacheRpcCommand command) {
      if (command instanceof TopologyAffectedCommand topologyAffectedCommand) {
         if (topologyAffectedCommand.getTopologyId() == -1) {
            int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
            if (log.isTraceEnabled()) {
               log.tracef("Topology id missing on command %s, setting it to %d", command, currentTopologyId);
            }
            topologyAffectedCommand.setTopologyId(currentTopologyId);
         }
      }
   }

   // -------------------------------------------- JMX information -----------------------------------------------

   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      replicationCount.reset();
      replicationFailures.reset();
      totalReplicationTime.reset();
      xSiteMetricsCollector.resetRequestsSent();
      xSiteMetricsCollector.resetRequestReceived();
   }

   @ManagedAttribute(description = "Number of successful replications", displayName = "Number of successful replications", measurementType = MeasurementType.TRENDSUP)
   public long getReplicationCount() {
      return isStatisticsEnabled() ? replicationCount.sum() : -1;
   }

   @ManagedAttribute(description = "Number of failed replications", displayName = "Number of failed replications", measurementType = MeasurementType.TRENDSUP)
   public long getReplicationFailures() {
      return isStatisticsEnabled() ? replicationFailures.sum() : -1;
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", dataType = DataType.TRAIT, writable = true)
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @Override
   public boolean getStatisticsEnabled() {
      return isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications", displayName = "Successful replications ratio")
   public String getSuccessRatio() {
      double ration;
      if (isStatisticsEnabled() && (ration = calculateSuccessRatio()) != 0) {
         return NumberFormat.getInstance().format(ration * 100d) + "%";
      }
      return "N/A";
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications in numeric double format", displayName = "Successful replication ratio", units = Units.PERCENTAGE)
   public double getSuccessRatioFloatingPoint() {
      return isStatisticsEnabled() ? calculateSuccessRatio() : 0;
   }

   private double calculateSuccessRatio() {
      double totalCount = replicationCount.sum() + replicationFailures.sum();
      return totalCount == 0 ? 0 : replicationCount.sum() / totalCount;
   }

   @ManagedAttribute(description = "The average time spent in the transport layer, in milliseconds", displayName = "Average time spent in the transport layer", units = Units.MILLISECONDS)
   public long getAverageReplicationTime() {
      long count = replicationCount.sum();
      return isStatisticsEnabled() && count != 0 ? totalReplicationTime.sum() / count : 0;
   }

   @ManagedAttribute(description = "Retrieves the x-site view.", displayName = "Cross site (x-site) view", dataType = DataType.TRAIT)
   public String getSitesView() {
      Set<String> sitesView = t.getSitesView();
      return sitesView != null ? sitesView.toString() : "N/A";
   }

   @ManagedAttribute(description = "Returns the average replication time, in milliseconds, for a cross-site replication request",
         displayName = "Average Cross-Site replication time",
         units = Units.MILLISECONDS)
   public long getAverageXSiteReplicationTime() {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getAvgRequestSentDuration(-1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedOperation(description = "Returns the average replication time, in milliseconds, for cross-site request sent to the remote site.",
         displayName = "Average Cross-Site replication time to Site",
         name = "AverageXSiteReplicationTimeTo")
   public long getAverageXSiteReplicationTimeTo(
         @Parameter(name = "dstSite", description = "Destination site name") String dstSite) {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getAvgRequestSentDuration(dstSite, -1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedAttribute(description = "Returns the minimum replication time, in milliseconds, for a cross-site replication request",
         displayName = "Minimum Cross-Site replication time",
         units = Units.MILLISECONDS,
         measurementType = MeasurementType.TRENDSDOWN)
   public long getMinimumXSiteReplicationTime() {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getMinRequestSentDuration(-1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedOperation(description = "Returns the minimum replication time, in milliseconds, for cross-site request sent to the remote site.",
         displayName = "Minimum Cross-Site replication time to Site",
         name = "MinimumXSiteReplicationTimeTo")
   public long getMinimumXSiteReplicationTimeTo(
         @Parameter(name = "dstSite", description = "Destination site name") String dstSite) {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getMinRequestSentDuration(dstSite, -1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedAttribute(description = "Returns the maximum replication time, in milliseconds, for a cross-site replication request",
         displayName = "Maximum Cross-Site replication time",
         units = Units.MILLISECONDS,
         measurementType = MeasurementType.TRENDSUP)
   public long getMaximumXSiteReplicationTime() {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getMaxRequestSentDuration(-1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedOperation(description = "Returns the maximum replication time, in milliseconds, for cross-site request sent to the remote site.",
         displayName = "Maximum Cross-Site replication time to Site",
         name = "MaximumXSiteReplicationTimeTo")
   public long getMaximumXSiteReplicationTimeTo(
         @Parameter(name = "dstSite", description = "Destination site name") String dstSite) {
      return isStatisticsEnabled() ?
             xSiteMetricsCollector.getMaxRequestSentDuration(dstSite, -1, TimeUnit.MILLISECONDS) :
             -1;
   }

   @ManagedAttribute(description = "Returns the number of sync cross-site requests",
         displayName = "Cross-Site replication requests",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberXSiteRequests() {
      return isStatisticsEnabled() ? xSiteMetricsCollector.countRequestsSent() : 0;
   }

   @ManagedOperation(description = "Returns the number of cross-site requests sent to the remote site.",
         displayName = "Number of Cross-Site request sent to site",
         name = "NumberXSiteRequestsSentTo")
   public long getNumberXSiteRequestsSentTo(
         @Parameter(name = "dstSite", description = "Destination site name") String dstSite) {
      return isStatisticsEnabled() ? xSiteMetricsCollector.countRequestsSent(dstSite) : 0;
   }

   @ManagedAttribute(description = "Returns the number of cross-site requests received from all nodes",
         displayName = "Number of Cross-Site Requests Received from all sites",
         measurementType = MeasurementType.TRENDSUP)
   public long getNumberXSiteRequestsReceived() {
      return isStatisticsEnabled() ? xSiteMetricsCollector.countRequestsReceived() : 0;
   }

   @ManagedOperation(description = "Returns the number of cross-site requests received from the remote site.",
         displayName = "Number of Cross-Site request received from site",
         name = "NumberXSiteRequestsReceivedFrom")
   public long getNumberXSiteRequestsReceivedFrom(
         @Parameter(name = "srcSite", description = "Originator site name") String srcSite) {
      return isStatisticsEnabled() ? xSiteMetricsCollector.countRequestsReceived(srcSite) : 0;
   }

   // mainly for unit testing
   public void setTransport(Transport t) {
      this.t = t;
   }

   @Override
   public Address getAddress() {
      return t != null ? t.getAddress() : null;
   }

   @Override
   public int getTopologyId() {
      CacheTopology cacheTopology = distributionManager.getCacheTopology();
      return cacheTopology != null ? cacheTopology.getTopologyId() : -1;
   }

   @Override
   public RpcOptions getSyncRpcOptions() {
      return syncRpcOptions;
   }

   @Override
   public List<Address> getMembers() {
      return distributionManager.getCacheTopology().getMembers();
   }

   private CacheRpcCommand initRpcCommand(CacheRpcCommand command) {
      assert !(command instanceof TopologyAffectedCommand topologyCmd) ||
            topologyCmd.getTopologyId() >= 0 ||
            command instanceof InvalidateL1Command;

      command.setSpanAttributes(clusterSpanAttributes);
      return command;
   }
}
