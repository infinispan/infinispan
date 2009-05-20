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

import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This component really is just a wrapper around a {@link org.infinispan.remoting.transport.Transport} implementation,
 * and is used to set up the transport and provide lifecycle and dependency hooks into external transport
 * implementations.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@MBean(objectName = "RpcManager")
public class RpcManagerImpl implements RpcManager {

   private static final Log log = LogFactory.getLog(RpcManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport t;
   private final AtomicLong replicationCount = new AtomicLong(0);
   private final AtomicLong replicationFailures = new AtomicLong(0);
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
   }

   private boolean useReplicationQueue(boolean sync) {
      return !sync && replicationQueue != null && replicationQueue.isEnabled();
   }

   public final List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) throws Exception {
      List<Address> members = t.getMembers();
      if (members.size() < 2) {
         if (log.isDebugEnabled())
            log.debug("We're the only member in the cluster; Don't invoke remotely.");
         return Collections.emptyList();
      } else {
         try {
            List<Response> result = t.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, stateTransferEnabled);
            if (isStatisticsEnabled()) replicationCount.incrementAndGet();
            return result;
         } catch (CacheException e) {
            if (log.isTraceEnabled()) log.trace("replicaiton exception: ", e);
            if (isStatisticsEnabled()) replicationFailures.incrementAndGet();
            throw e;
         } catch (Throwable th) {
            log.error("unexpected error while replicating", th);
            if (isStatisticsEnabled()) replicationFailures.incrementAndGet();
            throw new CacheException(th);
         }
      }
   }

   public final List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) throws Exception {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, null);
   }

   public final List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, false, null);
   }

   public void retrieveState(String cacheName, long timeout) throws StateTransferException {
      if (t.isSupportStateTransfer()) {
         // TODO make these configurable
         Random r = new Random();
         int initialWaitTime = (r.nextInt(10) + 1) * 100; // millis
         int waitTimeIncreaseFactor = 2;
         int numRetries = 5;
         List<Address> members = t.getMembers();
         if (members.size() < 2) {
            if (log.isDebugEnabled())
               log.debug("We're the only member in the cluster; no one to retrieve state from. Not doing anything!");
            return;
         }

         boolean success = false;

         try {

            outer:
            for (int i = 0, wait = initialWaitTime; i < numRetries; i++) {
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
         anycastRpcCommand(null, rpc, sync, usePriorityQueue);
      }
   }

   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      broadcastRpcCommandInFuture(rpc, false, l);
   }

   public final void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> l) {
      anycastRpcCommandInFuture(null, rpc, usePriorityQueue, l);
   }

   public final void anycastRpcCommand(List<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException {
      anycastRpcCommand(recipients, rpc, sync, false);
   }

   public final void anycastRpcCommand(List<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
      if (trace) {
         log.trace("Broadcasting call " + rpc + " to recipient list " + recipients);
      }

      if (useReplicationQueue(sync)) {
         replicationQueue.add(rpc);
      } else {
         if (!(rpc instanceof CacheRpcCommand)) {
            rpc = cf.buildSingleRpcCommand(rpc);
         }
         List rsps;
         try {
            rsps = invokeRemotely(recipients, rpc, getResponseMode(sync),
                                  configuration.getSyncReplTimeout(), usePriorityQueue);
            if (trace) log.trace("responses=" + rsps);
            if (sync) checkResponses(rsps);
         } catch (CacheException e) {
            log.error("Replication exception", e);
            throw e;
         } catch (Exception ex) {
            log.error("Unexpected exception", ex);
            throw new ReplicationException("Unexpected exception while replicating", ex);
         }
      }
   }

   public final void anycastRpcCommandInFuture(List<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> l) {
      anycastRpcCommandInFuture(recipients, rpc, false, l);
   }

   public final void anycastRpcCommandInFuture(final List<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> l) {
      Callable<Object> c = new Callable<Object>() {
         public Object call() {
            anycastRpcCommand(recipients, rpc, true, usePriorityQueue);
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
      return sync ? ResponseMode.SYNCHRONOUS : configuration.isUseAsyncMarshalling() ? ResponseMode.ASYNCHRONOUS : ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING;
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

   @ManagedOperation
   public void resetStatistics() {
      replicationCount.set(0);
      replicationFailures.set(0);
   }

   @ManagedAttribute(description = "number of successful replications")
   public String getReplicationCount() {
      if (!isStatisticsEnabled()) {
         return "N/A";
      }
      return String.valueOf(replicationCount.get());
   }

   @ManagedAttribute(description = "number of failed replications")
   public String getReplicationFailures() {
      if (!isStatisticsEnabled()) {
         return "N/A";
      }
      return String.valueOf(replicationFailures.get());
   }

   @ManagedAttribute(description = "whether or not jmx statistics are enabled")
   public boolean isStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedAttribute
   public void setStatisticsEnabled(boolean statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
   }

   @ManagedAttribute
   public String getAddress() {
      if (t == null || !isStatisticsEnabled()) return "N/A";
      Address address = t.getAddress();
      return address == null ? "N/A" : address.toString();
   }

   @ManagedAttribute
   public String getMembers() {
      if (t == null || !isStatisticsEnabled()) return "N/A";
      List<Address> addressList = t.getMembers();
      return addressList.toString();
   }

   @ManagedAttribute
   public String getSuccessRatio() {
      if (replicationCount.get() == 0 || !statisticsEnabled) {
         return "N/A";
      }
      double totalCount = replicationCount.get() + replicationFailures.get();
      double ration = (double) replicationCount.get() / totalCount * 100d;
      return NumberFormat.getInstance().format(ration) + "%";
   }

   // mainly for unit testing
   public void setTransport(Transport t) {
      this.t = t;
   }
}
