/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.remoting.rpc;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.text.NumberFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

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
public class RpcManagerImpl implements RpcManager {

   private static final Log log = LogFactory.getLog(RpcManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport t;
   private final AtomicLong replicationCount = new AtomicLong(0);
   private final AtomicLong replicationFailures = new AtomicLong(0);
   private final AtomicLong totalReplicationTime = new AtomicLong(0);

   private boolean statisticsEnabled = false; // by default, don't gather statistics.
   private Configuration configuration;
   private ReplicationQueue replicationQueue;
   private ExecutorService asyncExecutor;
   private CommandsFactory cf;
   private LocalTopologyManager localTopologyManager;
   private StateTransferManager stateTransferManager;
   private String cacheName;

   @Inject
   public void injectDependencies(Transport t, Cache cache, Configuration cfg,
            ReplicationQueue replicationQueue, CommandsFactory cf,
            @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService e,
            LocalTopologyManager localTopologyManager, StateTransferManager stateTransferManager) {
      this.t = t;
      this.configuration = cfg;
      this.cacheName = cache.getName();
      this.replicationQueue = replicationQueue;
      this.asyncExecutor = e;
      this.cf = cf;
      this.localTopologyManager = localTopologyManager;
      this.stateTransferManager = stateTransferManager;
   }

   @Start(priority = 9)
   private void start() {
      statisticsEnabled = configuration.jmxStatistics().enabled();

      if (configuration.transaction().transactionProtocol().isTotalOrder())
         t.checkTotalOrderSupported(configuration.clustering().cacheMode().isDistributed());
   }

   @ManagedAttribute(description = "Retrieves the committed view.", displayName = "Committed view", dataType = DataType.TRAIT)
   public String getCommittedViewAsString() {
      return localTopologyManager == null ? "N/A" : String.valueOf(localTopologyManager.getCacheTopology(cacheName)
            .getCurrentCH());
   }

   @ManagedAttribute(description = "Retrieves the pending view.", displayName = "Pending view", dataType = DataType.TRAIT)
   public String getPendingViewAsString() {
      return localTopologyManager == null ? "N/A" : String.valueOf(localTopologyManager.getCacheTopology(cacheName)
            .getPendingCH());
   }

   private boolean useReplicationQueue(boolean sync) {
      return !sync && replicationQueue != null && replicationQueue.isEnabled();
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      RpcOptions options = getRpcOptionsBuilder(mode, !usePriorityQueue)
            .timeout(timeout, TimeUnit.MILLISECONDS).responseFilter(responseFilter).build();
      return invokeRemotely(recipients, rpcCommand, options);
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, null);
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, false, null);
   }

   @Override
   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      broadcastRpcCommand(rpc, sync, false);
   }

   @Override
   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
      } else {
         invokeRemotely(null, rpc, sync, usePriorityQueue);
      }
   }

   @Override
   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      broadcastRpcCommandInFuture(rpc, false, l);
   }

   @Override
   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(null, rpc, usePriorityQueue, l);
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      return invokeRemotely(recipients, rpc, sync, false);
   }

   @Override
   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      return invokeRemotely(recipients, rpc, sync, usePriorityQueue, configuration.clustering().sync().replTimeout());
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, long timeout) throws RpcException {
      ResponseMode responseMode = getResponseMode(sync);
      return invokeRemotely(recipients, rpc, sync, usePriorityQueue, timeout, responseMode);
   }

   private Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, long timeout, ResponseMode responseMode) {
      if (trace) log.tracef("%s broadcasting call %s to recipient list %s", t.getAddress(), rpc, recipients);

      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
         return null;
      } else {
         if (!(rpc instanceof CacheRpcCommand)) {
            rpc = cf.buildSingleRpcCommand(rpc);
         }
         Map<Address, Response> rsps = invokeRemotely(recipients, rpc, responseMode, timeout, usePriorityQueue);
         if (trace) log.tracef("Response(s) to %s is %s", rpc, rsps);
         if (sync) checkResponses(rsps);
         return rsps;
      }
   }

   @Override
   public final void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, false, l);
   }

   @Override
   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, l, configuration.clustering().sync().replTimeout());
   }

   @Override
   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l, final long timeout) {
      invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, l, timeout, false);
   }

   @Override
   public void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc,
                                      final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l,
                                      final long timeout, final boolean ignoreLeavers) {
      if (trace) log.tracef("%s invoking in future call %s to recipient list %s", t.getAddress(), rpc, recipients);
      final ResponseMode responseMode = ignoreLeavers ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.SYNCHRONOUS;
      final CountDownLatch futureSet = new CountDownLatch(1);
      Callable<Object> c = new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            Object result = null;
            try {
               result = invokeRemotely(recipients, rpc, true, usePriorityQueue, timeout, responseMode);
            } finally {
               try {
                  futureSet.await();
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } finally {
                  l.notifyDone();
               }
            }
            return result;
         }
      };
      l.setNetworkFuture(asyncExecutor.submit(c));
      futureSet.countDown();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
      if (trace) log.tracef("%s invoking %s to recipient list %s with options %s", t.getAddress(), rpc, recipients, options);

      //skip replication queue option was added because when the ReplicationQueue invokes remotely, the command was
      //added to the queue again. this way, we break the cycle
      if (!options.skipReplicationQueue() && useReplicationQueue(options.responseMode().isSynchronous())) {
         if (trace) {
            log.tracef("Using replication queue for command [%s]", rpc);
         }
         replicationQueue.add(rpc);
         return null;
      }
      if (!configuration.clustering().cacheMode().isClustered())
         throw new IllegalStateException("Trying to invoke a remote command but the cache is not clustered");
      if (!(rpc instanceof CacheRpcCommand)) {
         rpc = cf.buildSingleRpcCommand(rpc);
      }
      long startTimeNanos = 0;
      if (statisticsEnabled) startTimeNanos = System.nanoTime();
      try {
         // TODO Re-enable the filter (and test MirrsingRpcDispatcherTest) after we find a way to update the cache members list before state transfer has started
         // add a response filter that will ensure we don't wait for replies from non-members
         // but only if the target is the whole cluster and the call is synchronous
         // if strict peer-to-peer is enabled we have to wait for replies from everyone, not just cache members
//            if (recipients == null && mode.isSynchronous() && !globalCfg.transport().strictPeerToPeer()) {
//               // TODO Could improve performance a tiny bit by caching the members in RpcManagerImpl
//               Collection<Address> cacheMembers = localTopologyManager.getCacheTopology(cacheName).getMembers();
//               // the filter won't work if there is no other member in the cache, so we have to
//               if (cacheMembers.size() < 2) {
//                  log.tracef("We're the only member of cache %s; Don't invoke remotely.", cacheName);
//                  return Collections.emptyMap();
//               }
//               // if there is already a response filter attached it means it must have its own way of dealing with non-members
//               // so skip installing the filter
//               if (responseFilter == null) {
//                  responseFilter = new IgnoreExtraResponsesValidityFilter(cacheMembers, getAddress());
//               }
//            }
         if (rpc instanceof TopologyAffectedCommand) {
            TopologyAffectedCommand topologyAffectedCommand = (TopologyAffectedCommand) rpc;
            if (topologyAffectedCommand.getTopologyId() == -1) {
               topologyAffectedCommand.setTopologyId(stateTransferManager.getCacheTopology().getTopologyId());
            }
         }
         Map<Address, Response> result = t.invokeRemotely(recipients, rpc, options.responseMode(), options.timeUnit().toMillis(options.timeout()),
                                                          !options.fifoOrder(), options.responseFilter(), options.totalOrder(),
                                                          configuration.clustering().cacheMode().isDistributed());
         if (statisticsEnabled) replicationCount.incrementAndGet();
         if (trace) log.tracef("Response(s) to %s is %s", rpc, result);
         if (options.responseMode().isSynchronous()) {
            checkResponses(result);
         }
         return result;
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
            long timeTaken = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
            totalReplicationTime.getAndAdd(timeTaken);
         }
      }
   }

   @Override
   public void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc,
                                      final RpcOptions options, final NotifyingNotifiableFuture<Object> future) {
      if (trace) log.tracef("%s invoking in future call %s to recipient list %s with options &s", t.getAddress(),
                            rpc, recipients, options);

      final CountDownLatch futureSet = new CountDownLatch(1);
      Callable<Object> c = new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            Object result = null;
            try {
               result = invokeRemotely(recipients, rpc, options);
            } finally {
               try {
                  futureSet.await();
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } finally {
                  future.notifyDone();
               }
            }
            return result;
         }
      };
      future.setNetworkFuture(asyncExecutor.submit(c));
      futureSet.countDown();
   }

   @Override
   public Transport getTransport() {
      return t;
   }

   private ResponseMode getResponseMode(boolean sync) {
      return sync ? ResponseMode.SYNCHRONOUS : ResponseMode.getAsyncResponseMode(configuration);
   }

   /**
    * Checks whether any of the responses are exceptions. If yes, re-throws them (as exceptions or runtime exceptions).
    */
   private void checkResponses(Map<Address, Response> rsps) {
      if (rsps != null) {
         for (Map.Entry<Address, Response> rsp : rsps.entrySet()) {
            // TODO Double-check this logic, rsp.getValue() is a Response so it's 100% not Throwable
            if (rsp != null && rsp.getValue() instanceof Throwable) {
               Throwable throwable = (Throwable) rsp.getValue();
               if (trace)
                  log.tracef("Received Throwable from remote node %s", throwable, rsp.getKey());
               throw new RpcException(throwable);
            }
         }
      }
   }

   // -------------------------------------------- JMX information -----------------------------------------------

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

   @ManagedAttribute(description = "Statistics enabled", displayName = "Statistics enabled", dataType = DataType.TRAIT, writable = true)
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   /**
    * @deprecated We already have an attribute, we shouldn't have an operation for the same thing.
    */
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
      return getRpcOptionsBuilder(responseMode, true);
   }

   @Override
   public RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, boolean fifoOrder) {
      return new RpcOptionsBuilder(configuration.clustering().sync().replTimeout(), TimeUnit.MILLISECONDS, responseMode,
                                   fifoOrder);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync) {
      return getDefaultRpcOptions(sync, true);
   }

   @Override
   public RpcOptions getDefaultRpcOptions(boolean sync, boolean fifoOrder) {
      return getRpcOptionsBuilder(sync ? ResponseMode.SYNCHRONOUS : ResponseMode.getAsyncResponseMode(configuration),
                                  fifoOrder).build();
   }

   @Override
   public List<Address> getMembers() {
      return stateTransferManager.getCacheTopology().getMembers();
   }
}
