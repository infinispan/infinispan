package org.infinispan.remoting.rpc;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.ReplicationException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This component really is just a wrapper around a {@link org.infinispan.remoting.transport.Transport} implementation,
 * and is used to set up the transport and provide lifecycle and dependency hooks into external transport
 * implementations.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
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


   @Inject
   public void injectDependencies(Transport t, Configuration configuration, ReplicationQueue replicationQueue, CommandsFactory cf,
                                  @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e) {
      this.t = t;
      this.configuration = configuration;
      this.replicationQueue = replicationQueue;
      this.asyncExecutor = e;
      this.cf = cf;
   }

   @Start(priority = 9)
   private void start() {
      stateTransferEnabled = configuration.isStateTransferEnabled();
      statisticsEnabled = configuration.isExposeJmxStatistics();
   }

   private boolean useReplicationQueue(boolean sync) {
      return !sync && replicationQueue != null && replicationQueue.isEnabled();
   }

   public final List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
      List<Address> members = t.getMembers();
      if (members.size() < 2) {
         if (log.isDebugEnabled())
            log.debug("We're the only member in the cluster; Don't invoke remotely.");
         return Collections.emptyList();
      } else {
         long startTime = 0;
         if (statisticsEnabled) startTime = System.currentTimeMillis();
         try {
            List<Response> result = t.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, stateTransferEnabled);
            if (isStatisticsEnabled()) replicationCount.incrementAndGet();
            return result;
         } catch (CacheException e) {
            if (log.isTraceEnabled()) {
               log.trace("replication exception: ", e);
            }

            if (isStatisticsEnabled()) replicationFailures.incrementAndGet();
            throw e;
         } catch (Throwable th) {
            log.error("unexpected error while replicating", th);
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

   public final List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, null);
   }

   public final List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception {
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
         }

         boolean success = false;

         try {
            long wait = initialWaitTime;
            outer:
            for (int i = 0; i < numRetries; i++) {
               for (Address member : members) {
                  if (!member.equals(t.getAddress())) {
                     try {
                        if (log.isInfoEnabled()) log.info("Trying to fetch state from {0}", member);
                        currentStateTransferSource = member;
                        if (t.retrieveState(cacheName, member, timeout)) {
                           if (log.isInfoEnabled())
                              log.info("Successfully retrieved and applied state from {0}", member);
                           success = true;
                           break outer;
                        }
                     } catch (StateTransferException e) {
                        if (log.isDebugEnabled()) log.debug("Error while fetching state from member " + member, e);
                     } finally {
                        currentStateTransferSource = null;
                     }
                  }
               }

               if (!success) {
                  if (log.isWarnEnabled())
                     log.warn("Could not find available peer for state, backing off and retrying");

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

   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws ReplicationException {
      broadcastRpcCommand(rpc, sync, false);
   }

   public final void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
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

   public final void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException {
      invokeRemotely(recipients, rpc, sync, false);
   }

   public final void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
      invokeRemotely(recipients, rpc, sync, usePriorityQueue, configuration.getSyncReplTimeout());
   }

   public final void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue, long timeout) throws ReplicationException {
      if (trace) log.trace("{0} broadcasting call {1} to recipient list {2}", t.getAddress(), rpc, recipients);

      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
      } else {
         if (!(rpc instanceof CacheRpcCommand)) {
            rpc = cf.buildSingleRpcCommand(rpc);
         }
         List rsps;
         rsps = invokeRemotely(recipients, rpc, getResponseMode(sync), timeout, usePriorityQueue);
         if (trace) log.trace("responses=" + rsps);
         if (sync) checkResponses(rsps);
      }
   }

   public final void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, false, l);
   }

   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l) {
      invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, l, configuration.getSyncReplTimeout());
   }

   public final void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l, final long timeout) {
      if (trace) log.trace("{0} invoking in future call {1} to recipient list {2}", t.getAddress(), rpc, recipients); 
      Callable<Object> c = new Callable<Object>() {
         public Object call() {
            invokeRemotely(recipients, rpc, true, usePriorityQueue, timeout);
            l.notifyDone();
            return null;
         }
      };
      l.setNetworkFuture(asyncExecutor.submit(c));
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
   private void checkResponses(List rsps) {
      if (rsps != null) {
         for (Object rsp : rsps) {
            if (rsp != null && rsp instanceof Throwable) {
               // lets print a stack trace first.
               Throwable throwable = (Throwable) rsp;
               if (trace) {
                  log.trace("Received Throwable from remote cache", throwable);
               }
               throw new ReplicationException(throwable);
            }
         }
      }
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
   @Metric(displayName = "Number of successfull replications", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
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

   @ManagedAttribute(description = "The network address associated with this instance")
   @Metric(displayName = "Network address", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getNodeAddress() {
      if (t == null || !isStatisticsEnabled()) return "N/A";
      Address address = t.getAddress();
      return address == null ? "N/A" : address.toString();
   }

   @ManagedAttribute(description = "The physical network addresses associated with this instance")
   @Metric(displayName = "Physical network addresses", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getPhysicalAddresses() {
      if (t == null || !isStatisticsEnabled()) return "N/A";
      List<Address> address = t.getPhysicalAddresses();
      return address == null ? "N/A" : address.toString();
   }

   @ManagedAttribute(description = "List of members in the cluster")
   @Metric(displayName = "Cluster members", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getMembers() {
      if (t == null || !isStatisticsEnabled()) return "N/A";
      List<Address> addressList = t.getMembers();
      return addressList.toString();
   }

   @ManagedAttribute(description = "Size of the cluster in number of nodes")
   @Metric(displayName = "Cluster size", displayType = DisplayType.SUMMARY)
   public int getClusterSize() {
      return t.getMembers().size();
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
