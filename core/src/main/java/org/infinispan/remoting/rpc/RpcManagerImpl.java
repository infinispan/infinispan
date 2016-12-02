package org.infinispan.remoting.rpc;

import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferManager;
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

   private Transport t;
   private final AtomicLong replicationCount = new AtomicLong(0);
   private final AtomicLong replicationFailures = new AtomicLong(0);
   private final AtomicLong totalReplicationTime = new AtomicLong(0);

   private boolean statisticsEnabled = false; // by default, don't gather statistics.
   private Configuration configuration;
   private CommandsFactory cf;
   private StateTransferManager stateTransferManager;
   private TimeService timeService;

   @Inject
   public void injectDependencies(Transport t, Configuration cfg, CommandsFactory cf,
                                  StateTransferManager stateTransferManager, TimeService timeService) {
      this.t = t;
      this.configuration = cfg;
      this.cf = cf;
      this.stateTransferManager = stateTransferManager;
      this.timeService = timeService;
   }

   @Start(priority = 9)
   private void start() {
      statisticsEnabled = configuration.jmxStatistics().enabled();

      if (configuration.transaction().transactionProtocol().isTotalOrder())
         t.checkTotalOrderSupported();
   }

   @ManagedAttribute(description = "Retrieves the committed view.", displayName = "Committed view", dataType = DataType.TRAIT)
   public String getCommittedViewAsString() {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      if (cacheTopology == null)
         return "N/A";

      return cacheTopology.getCurrentCH().getMembers().toString();
   }

   @ManagedAttribute(description = "Retrieves the pending view.", displayName = "Pending view", dataType = DataType.TRAIT)
   public String getPendingViewAsString() {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      if (cacheTopology == null)
         return "N/A";

      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      return pendingCH != null ? pendingCH.getMembers().toString() : "null";
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpc,
                                                                        RpcOptions options) {
      if (trace) log.tracef("%s invoking %s to recipient list %s with options %s", t.getAddress(), rpc, recipients, options);

      if (!configuration.clustering().cacheMode().isClustered())
         throw new IllegalStateException("Trying to invoke a remote command but the cache is not clustered");

      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(rpc);

      CacheRpcCommand cacheRpc =
            rpc instanceof CacheRpcCommand ? (CacheRpcCommand) rpc : cf.buildSingleRpcCommand(rpc);

      long startTimeNanos = statisticsEnabled ? timeService.time() : 0;
      CompletableFuture<Map<Address, Response>> invocation;
      try {
         invocation = t.invokeRemotelyAsync(recipients, cacheRpc,
               options.responseMode(), options.timeUnit().toMillis(options.timeout()),
               options.responseFilter(), options.deliverOrder(),
               configuration.clustering().cacheMode().isDistributed());
      } catch (Exception e) {
         log.unexpectedErrorReplicating(e);
         if (statisticsEnabled) replicationFailures.incrementAndGet();
         return rethrowAsCacheException(e);
      }
      return invocation.handle((responseMap, throwable) -> {
         if (statisticsEnabled) {
            long timeTaken = timeService.timeDuration(startTimeNanos, TimeUnit.MILLISECONDS);
            totalReplicationTime.getAndAdd(timeTaken);
         }

         if (throwable == null) {
            if (statisticsEnabled) replicationCount.incrementAndGet();
            if (trace) log.tracef("Response(s) to %s is %s", rpc, responseMap);
            return responseMap;
         } else {
            if (statisticsEnabled) replicationFailures.incrementAndGet();
            return rethrowAsCacheException(throwable);
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
      CompletableFuture<Map<Address, Response>> future = invokeRemotelyAsync(recipients, rpc, options);
      try {
         return CompletableFutures.await(future);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException("Thread interrupted while invoking RPC", e);
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         if (cause instanceof CacheException) {
            throw ((CacheException) cause);
         } else {
            throw new CacheException("Unexpected exception replicating command", cause);
         }
      }
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options) {
      if (trace) log.tracef("%s invoking %s with options %s", t.getAddress(), rpcs, options);

      // don't use replication queue as we don't want to send the command to all other nodes
      if (!configuration.clustering().cacheMode().isClustered())
         throw new IllegalStateException("Trying to invoke a remote command but the cache is not clustered");

      Map<Address, ReplicableCommand> replacedCommands = null;
      for (Map.Entry<Address, ReplicableCommand> entry : rpcs.entrySet()) {
         ReplicableCommand rpc = entry.getValue();
         // Set the topology id of the command, in case we don't have it yet
         setTopologyId(rpc);
         if (!(rpc instanceof CacheRpcCommand)) {
            rpc = cf.buildSingleRpcCommand(rpc);
            // we can't modify the map during iteration
            if (replacedCommands == null) {
               replacedCommands = new HashMap<>();
            }
            replacedCommands.put(entry.getKey(), rpc);
         }
      }
      if (replacedCommands != null) {
         rpcs.putAll(replacedCommands);
      }

      long startTimeNanos = 0;
      if (statisticsEnabled) startTimeNanos = timeService.time();
      try {
         Map<Address, Response> result = t.invokeRemotely(rpcs, options.responseMode(), options.timeUnit().toMillis(options.timeout()),
               options.responseFilter(), options.deliverOrder(), configuration.clustering().cacheMode().isDistributed());
         if (statisticsEnabled) replicationCount.incrementAndGet();
         if (trace) log.tracef("Response(s) to %s is %s", rpcs, result);
         return result;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException("Thread interrupted while invoking RPC", e);
      } catch (CacheException e) {
         log.trace("replication exception: ", e);
         if (statisticsEnabled) replicationFailures.incrementAndGet();
         throw e;
      } catch (Throwable th) {
         log.unexpectedErrorReplicating(th);
         if (statisticsEnabled) replicationFailures.incrementAndGet();
         throw new CacheException(th);
      } finally {
         if (statisticsEnabled) {
            long timeTaken = timeService.timeDuration(startTimeNanos, TimeUnit.MILLISECONDS);
            totalReplicationTime.getAndAdd(timeTaken);
         }
      }
   }

   private CacheRpcCommand toCacheRpcCommand(ReplicableCommand command) {
      return command instanceof CacheRpcCommand ?
            (CacheRpcCommand) command :
            cf.buildSingleRpcCommand(command);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder) {
      if (trace) {
         log.tracef("%s invoking %s to %s ordered by %s", t.getAddress(), command, destination, deliverOrder);
      }

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
      if (trace) {
         log.tracef("%s invoking %s to list %s ordered by %s", t.getAddress(), command, destinations, deliverOrder);
      }

      // Set the topology id of the command, in case we don't have it yet
      setTopologyId(command);
      CacheRpcCommand cacheRpc = toCacheRpcCommand(command);

      try {
         t.sendToMany(destinations, cacheRpc, deliverOrder);
      } catch (Exception e) {
         errorReplicating(e);
      }
   }

   private void errorReplicating(Exception e) {
      log.unexpectedErrorReplicating(e);
      if (statisticsEnabled) replicationFailures.incrementAndGet();
      rethrowAsCacheException(e);
   }

   @Override
   public Transport getTransport() {
      return t;
   }

   private void setTopologyId(ReplicableCommand command) {
      if (command instanceof TopologyAffectedCommand) {
         TopologyAffectedCommand topologyAffectedCommand = (TopologyAffectedCommand) command;
         if (topologyAffectedCommand.getTopologyId() == -1) {
            int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
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
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      return cacheTopology != null ? cacheTopology.getTopologyId() : -1;
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
      return stateTransferManager.getCacheTopology().getMembers();
   }
}
