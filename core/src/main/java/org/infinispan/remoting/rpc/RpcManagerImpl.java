package org.infinispan.remoting.rpc;

import java.text.NumberFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.util.logging.TraceException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeListener;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
@MBean(objectName = "RpcManager", description = "Manages all remote calls to remote cache instances in the cluster.")
public class RpcManagerImpl implements RpcManager, JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(RpcManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private Transport t;
   @Inject private Configuration configuration;
   @Inject private CommandsFactory cf;
   @Inject private DistributionManager distributionManager;
   @Inject private TimeService timeService;

   private final Function<ReplicableCommand, ReplicableCommand> toCacheRpcCommand = this::toCacheRpcCommand;
   private final AttributeListener<Long> updateRpcOptions = this::updateRpcOptions;

   private final AtomicLong replicationCount = new AtomicLong(0);
   private final AtomicLong replicationFailures = new AtomicLong(0);
   private final AtomicLong totalReplicationTime = new AtomicLong(0);

   private boolean statisticsEnabled = false; // by default, don't gather statistics.

   private volatile RpcOptions syncRpcOptions;
   private volatile RpcOptions totalSyncRpcOptions;


   @Start(priority = 9)
   private void start() {
      statisticsEnabled = configuration.jmxStatistics().enabled();

      if (configuration.transaction().transactionProtocol().isTotalOrder())
         t.checkTotalOrderSupported();

      configuration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener(updateRpcOptions);
      updateRpcOptions(configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT), null);
   }

   @Stop
   private void stop() {
      configuration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .removeListener(updateRpcOptions);
   }

   private void updateRpcOptions(Attribute<Long> attribute, Long oldValue) {
      syncRpcOptions = new RpcOptions(DeliverOrder.NONE, attribute.get(), TimeUnit.MILLISECONDS);
      totalSyncRpcOptions = new RpcOptions(DeliverOrder.TOTAL, attribute.get(), TimeUnit.MILLISECONDS);
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
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

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

   private void checkTopologyId(ReplicableCommand command) {
      if (command instanceof TopologyAffectedCommand && ((TopologyAffectedCommand) command).getTopologyId() < 0) {
         throw new IllegalArgumentException("Command does not have a topology id");
      }
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

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
      totalReplicationTime.getAndAdd(timeTaken);

      if (throwable == null) {
         if (statisticsEnabled)
            replicationCount.incrementAndGet();
         return response;
      } else {
         if (statisticsEnabled)
            replicationFailures.incrementAndGet();
         return rethrowAsCacheException(throwable);
      }
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);
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
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, RpcOptions rpcOptions) {
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

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
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, RpcOptions rpcOptions) {
      if (!statisticsEnabled) {
         return t.invokeCommands(targets, commandGenerator.andThen(toCacheRpcCommand), collector,
                                 rpcOptions.deliverOrder(), rpcOptions.timeout(), rpcOptions.timeUnit());
      }

      long startTimeNanos = timeService.time();
      CompletionStage<T> invocation;
      try {
         invocation = t.invokeCommands(targets, commandGenerator.andThen(toCacheRpcCommand), collector,
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

   @SuppressWarnings("deprecation")
   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpc,
                                                                        RpcOptions options) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(rpc);
      CacheRpcCommand cacheRpc =
            rpc instanceof CacheRpcCommand ? (CacheRpcCommand) rpc : cf.buildSingleRpcCommand(rpc);

      long startTimeNanos = statisticsEnabled ? timeService.time() : 0;
      CompletableFuture<Map<Address, Response>> invocation;
      try {
         // Using Transport.invokeCommand* would require us to duplicate the JGroupsTransport.invokeRemotelyAsync logic
         invocation = t.invokeRemotelyAsync(recipients, cacheRpc,
               options.responseMode(), options.timeUnit().toMillis(options.timeout()),
               options.responseFilter(), options.deliverOrder(),
               configuration.clustering().cacheMode().isDistributed());
      } catch (Exception e) {
         log.unexpectedErrorReplicating(e);
         if (statisticsEnabled) replicationFailures.incrementAndGet();
         return rethrowAsCacheException(e);
      }

      return invocation.whenComplete((responseMap, throwable) -> {
         if (statisticsEnabled) {
            updateStatistics(startTimeNanos, responseMap, throwable);
         }
      });
   }

   private <T> T rethrowAsCacheException(Throwable throwable) {
      if (throwable.getCause() != null && throwable instanceof CompletionException) {
         throwable = throwable.getCause();
      }
      if (throwable instanceof CacheException) {
         log.trace("Replication exception", throwable);
         throw ((CacheException) throwable);
      } else {
         log.unexpectedErrorReplicating(throwable);
         throw new CacheException(throwable);
      }
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      return blocking(invokeRemotelyAsync(recipients, rpc, options));
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      // don't use replication queue as we don't want to send the command to all other nodes
      if (!configuration.clustering().cacheMode().isClustered())
         throw new IllegalStateException("Trying to invoke a remote command but the cache is not clustered");

      if (options.responseMode().isAsynchronous()) {
         rpcs.forEach((address, command) -> sendTo(address, command, options.deliverOrder()));
      }

      try {
         Function<Address, ReplicableCommand> commandGenerator = address -> {
            ReplicableCommand rpc = rpcs.get(address);
            // Set the topology id of the command, in case we don't have it yet
            setTopologyId(rpc);
            return toCacheRpcCommand(rpc);
         };
         boolean ignoreLeavers = options.responseMode() == ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS;
         MapResponseCollector collector = MapResponseCollector.ignoreLeavers(ignoreLeavers, rpcs.size());
         return blocking(invokeCommands(rpcs.keySet(), commandGenerator, collector, options));
      } catch (CacheException e) {
         log.trace("replication exception: ", e);
         throw e;
      } catch (Throwable th) {
         return errorReplicating(th);
      }
   }

   private CacheRpcCommand toCacheRpcCommand(ReplicableCommand command) {
      checkTopologyId(command);
      return command instanceof CacheRpcCommand ?
            (CacheRpcCommand) command :
            cf.buildSingleRpcCommand(command);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

      try {
         t.sendTo(destination, cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);
      try {
         t.sendToMany(destinations, cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   @Override
   public void sendToAll(ReplicableCommand command, DeliverOrder deliverOrder) {
      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

      try {
         t.sendToAll(cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   private <T> T errorReplicating(Throwable t) {
      log.unexpectedErrorReplicating(t);
      if (statisticsEnabled) replicationFailures.incrementAndGet();
      return rethrowAsCacheException(t);
   }

   @Override
   public Transport getTransport() {
      return t;
   }

   private void setTopologyId(ReplicableCommand command) {
      if (command instanceof TopologyAffectedCommand) {
         TopologyAffectedCommand topologyAffectedCommand = (TopologyAffectedCommand) command;
         if (topologyAffectedCommand.getTopologyId() == -1) {
            int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
            if (trace) {
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
      replicationCount.set(0);
      replicationFailures.set(0);
      totalReplicationTime.set(0);
   }

   @ManagedAttribute(description = "Number of successful replications", displayName = "Number of successful replications", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getReplicationCount() {
      if (!isStatisticsEnabled()) {
         return -1;
      }
      return replicationCount.get();
   }

   @ManagedAttribute(description = "Number of failed replications", displayName = "Number of failed replications", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getReplicationFailures() {
      if (!isStatisticsEnabled()) {
         return -1;
      }
      return replicationFailures.get();
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", displayName = "Statistics enabled", dataType = DataType.TRAIT, writable = true)
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @Override
   public boolean getStatisticsEnabled() {
      return isStatisticsEnabled();
   }

   /**
    * @deprecated We already have an attribute, we shouldn't have an operation for the same thing.
    */
   @Override
   @Deprecated
   @ManagedOperation(displayName = "Enable/disable statistics. Deprecated, use the statisticsEnabled attribute instead.")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications", displayName = "Successful replications ratio")
   public String getSuccessRatio() {
      if (replicationCount.get() == 0 || !statisticsEnabled) {
         return "N/A";
      }
      double ration = calculateSuccessRatio() * 100d;
      return NumberFormat.getInstance().format(ration) + "%";
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications in numeric double format", displayName = "Successful replication ratio", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getSuccessRatioFloatingPoint() {
      if (replicationCount.get() == 0 || !statisticsEnabled) return 0;
      return calculateSuccessRatio();
   }

   private double calculateSuccessRatio() {
      double totalCount = replicationCount.get() + replicationFailures.get();
      return replicationCount.get() / totalCount;
   }

   @ManagedAttribute(description = "The average time spent in the transport layer, in milliseconds", displayName = "Average time spent in the transport layer", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
   public long getAverageReplicationTime() {
      if (replicationCount.get() == 0) {
         return 0;
      }
      return totalReplicationTime.get() / replicationCount.get();
   }

   @ManagedAttribute(description = "Retrieves the x-site view.", displayName = "Cross site (x-site) view", dataType = DataType.TRAIT)
   public String getSitesView() {
      Set<String> sitesView = t.getSitesView();
      return sitesView != null ? sitesView.toString() : "N/A";
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
   public RpcOptions getTotalSyncRpcOptions() {
      return totalSyncRpcOptions;
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode) {
      return getRpcOptionsBuilder(responseMode, responseMode.isSynchronous() ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder) {
      return new RpcOptionsBuilder(configuration.clustering().remoteTimeout(), TimeUnit.MILLISECONDS, responseMode, deliverOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return getDefaultRpcOptions(sync, sync ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder) {
      return getRpcOptionsBuilder(sync ? ResponseMode.SYNCHRONOUS : ResponseMode.ASYNCHRONOUS,
                                  deliverOrder).build();
   }

   @Override
   public List<Address> getMembers() {
      return distributionManager.getCacheTopology().getMembers();
   }
}
