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

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.factories.KnownComponentNames.*;

/**
 * This component really is just a wrapper around a {@link org.infinispan.remoting.transport.Transport} implementation,
 * and is used to set up the transport and provide lifecycle and dependency hooks into external transport
 * implementations.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
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

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   boolean statisticsEnabled = false; // by default, don't gather statistics.
   private volatile Address currentStateTransferSource;
   private boolean stateTransferEnabled;
   private Configuration configuration;
   private ReplicationQueue replicationQueue;
   private ExecutorService asyncExecutor;
   private CommandsFactory cf;
   private StreamingMarshaller marshaller;


   @Inject
   public void injectDependencies(Transport t, Configuration configuration, ReplicationQueue replicationQueue, CommandsFactory cf,
                                  @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService e,
                                  @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller) {
      this.t = t;
      this.configuration = configuration;
      this.replicationQueue = replicationQueue;
      this.asyncExecutor = e;
      this.cf = cf;
      this.marshaller = marshaller;
   }

   @Start(priority = 9)
   private void start() {
      stateTransferEnabled = configuration.isStateTransferEnabled();
      statisticsEnabled = configuration.isExposeJmxStatistics();
   }

   private boolean useReplicationQueue(boolean sync) {
      return !sync && replicationQueue != null && replicationQueue.isEnabled();
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      List<Address> members = t.getMembers();
      if (members.size() < 2) {
         if (log.isDebugEnabled())
            log.debug("We're the only member in the cluster; Don't invoke remotely.");
         return Collections.emptyMap();
      } else {
         long startTime = 0;
         if (statisticsEnabled) startTime = System.currentTimeMillis();
         try {
            Map<Address, Response> result = t.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, stateTransferEnabled);
            if (isStatisticsEnabled()) replicationCount.incrementAndGet();
            return result;
         } catch (CacheException e) {
            if (log.isTraceEnabled()) {
               log.trace("replication exception: ", e);
            }

            if (isStatisticsEnabled()) replicationFailures.incrementAndGet();
            throw e;
         } catch (Throwable th) {
            log.unexpectedErrorReplicating(th);
            if (isStatisticsEnabled()) replicationFailures.incrementAndGet();
            throw new CacheException(th);
         } finally {
            if (statisticsEnabled) {
               long timeTaken = System.currentTimeMillis() - startTime;
               totalReplicationTime.getAndAdd(timeTaken);
            }
         }
      }
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, null);
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, false, null);
   }

   public void retrieveState(String cacheName, long timeout) throws StateTransferException {
      if (t.isSupportStateTransfer()) {
         long initialWaitTime = configuration.getStateRetrievalInitialRetryWaitTime();
         int waitTimeIncreaseFactor = configuration.getStateRetrievalRetryWaitTimeIncreaseFactor();
         int numRetries = configuration.getStateRetrievalNumRetries();
         List<Address> members = t.getMembers();
         if (members.size() < 2) {
            if (log.isDebugEnabled())
               log.debug("We're the only member in the cluster; no one to retrieve state from. Not doing anything!");
            return;
         } else if (t.isCoordinator()) {
            if (log.isDebugEnabled())
               log.debug("We're the coordinator, so don't request state from other nodes in the cluster");
            return;
         }

         boolean success = false;

         try {
            long wait = initialWaitTime;
            outer:
            for (int i = 0; i < numRetries; i++) {
               for (Address member : members) {
                  if (!member.equals(t.getAddress())) {
                     try {
                        if (log.isInfoEnabled()) log.tryingToFetchState(member);
                        currentStateTransferSource = member;
                        mimicPartialFlushViaRPC(currentStateTransferSource, true);
                        if (t.retrieveState(cacheName, member, timeout)) {
                           if (log.isInfoEnabled())
                              log.successfullyAppliedState(member);
                           success = true;
                           break outer;
                        }
                     } catch (Exception ex){
                        if (log.isDebugEnabled()) log.debug("Error while fetching state from member " + member, ex);
                     }
                     finally {                        
                        try {
                           mimicPartialFlushViaRPC(currentStateTransferSource, false);
                        } catch (Exception ignored) {}
                        currentStateTransferSource = null;
                     }
                  }
               }

               if (!success) {
                  log.couldNotFindPeerForState();

                  try {
                     Thread.sleep(wait *= waitTimeIncreaseFactor);
                  }
                  catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                  }
               }

            }
         } finally {
            currentStateTransferSource = null;
         }

         if (!success) throw new StateTransferException("Unable to fetch state on startup");
      } else {
         throw new StateTransferException("Transport does not, or is not configured to, support state transfer.  Please disable fetching state on startup, or reconfigure your transport.");
      }
   }

   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException {
      broadcastRpcCommand(rpc, sync, false);
   }

   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
      } else {
         invokeRemotely(null, rpc, sync, usePriorityQueue);
      }
   }

   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      broadcastRpcCommandInFuture(rpc, false, l);
   }

   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(null, rpc, usePriorityQueue, l);
   }

   public final void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException {
      invokeRemotely(recipients, rpc, sync, false);
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException {
      return invokeRemotely(recipients, rpc, sync, usePriorityQueue, configuration.getSyncReplTimeout());
   }

   public final Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, long timeout) throws RpcException {
      if (trace) log.tracef("%s broadcasting call %s to recipient list %s", t.getAddress(), rpc, recipients);

      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
         return null;
      } else {
         if (!(rpc instanceof CacheRpcCommand)) {
            rpc = cf.buildSingleRpcCommand(rpc);
         }
         Map<Address, Response> rsps = invokeRemotely(recipients, rpc, getResponseMode(sync), timeout, usePriorityQueue);
         if (trace) log.tracef("Response(s) to %s is %s", rpc, rsps);
         if (sync) checkResponses(rsps);
         return rsps;
      }
   }

   public final void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, false, l);
   }

   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, l, configuration.getSyncReplTimeout());
   }

   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l, final long timeout) {
      if (trace) log.tracef("%s invoking in future call %s to recipient list %s", t.getAddress(), rpc, recipients);
      final CountDownLatch futureSet = new CountDownLatch(1);
      Callable<Object> c = new Callable<Object>() {
         public Object call() throws Exception {
            Object result = null;
            try {
               result = invokeRemotely(recipients, rpc, true, usePriorityQueue, timeout);           
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

   public Transport getTransport() {
      return t;
   }

   public Address getCurrentStateTransferSource() {
      return currentStateTransferSource;
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
            if (rsp != null && rsp.getValue() instanceof Throwable) {
               Throwable throwable = (Throwable) rsp.getValue();
               if (trace)
                  log.tracef("Received Throwable from remote node %s", throwable, rsp.getKey());
               throw new RpcException(throwable);
            }
         }
      }
   }
   
   /**
    * Mimics a partial flush between the current instance and the address to flush, by opening and closing the necessary
    * latches on both ends.
    *
    * @param addressToFlush address to flush in addition to the current address
    * @param block          if true, mimics setting a flush.  Otherwise, mimics un-setting a flush.
    * @throws Exception if there are issues
    */
   private void mimicPartialFlushViaRPC(Address addressToFlush, boolean block) {
      StateTransferControlCommand cmd = cf.buildStateTransferControlCommand(block);
      if (!block)
         getTransport().getDistributedSync().releaseSync();
      
      invokeRemotely(Collections.singletonList(addressToFlush), cmd, ResponseMode.SYNCHRONOUS,
               configuration.getStateRetrievalTimeout(), true);
      
      if (block)
         getTransport().getDistributedSync().acquireSync();
   }

   // -------------------------------------------- JMX information -----------------------------------------------

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      replicationCount.set(0);
      replicationFailures.set(0);
      totalReplicationTime.set(0);
   }

   @ManagedAttribute(description = "Number of successful replications")
   @Metric(displayName = "Number of successful replications", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getReplicationCount() {
      if (!isStatisticsEnabled()) {
         return -1;
      }
      return replicationCount.get();
   }

   @ManagedAttribute(description = "Number of failed replications")
   @Metric(displayName = "Number of failed replications", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
   public long getReplicationFailures() {
      if (!isStatisticsEnabled()) {
         return -1;
      }
      return replicationFailures.get();
   }

   @Metric(displayName = "Statistics enabled", dataType = DataType.TRAIT)
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @Operation(displayName = "Enable/disable statistics")
   public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications")
   public String getSuccessRatio() {
      if (replicationCount.get() == 0 || !statisticsEnabled) {
         return "N/A";
      }
      double ration = calculateSuccessRatio() * 100d;
      return NumberFormat.getInstance().format(ration) + "%";
   }

   @ManagedAttribute(description = "Successful replications as a ratio of total replications in numeric double format")
   @Metric(displayName = "Successful replication ratio", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getSuccessRatioFloatingPoint() {
      if (replicationCount.get() == 0 || !statisticsEnabled) return 0;
      return calculateSuccessRatio();
   }

   private double calculateSuccessRatio() {
      double totalCount = replicationCount.get() + replicationFailures.get();
      return replicationCount.get() / totalCount;
   }

   @ManagedAttribute(description = "The average time spent in the transport layer, in milliseconds")
   @Metric(displayName = "Average time spent in the transport layer", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
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
}
