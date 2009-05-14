package org.infinispan.remoting.rpc;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.marshall.Marshaller;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

   Transport t;
   private final AtomicLong replicationCount = new AtomicLong(0);
   private final AtomicLong replicationFailures = new AtomicLong(0);
   boolean statisticsEnabled = false; // by default, don't gather statistics.
   private volatile Address currentStateTransferSource;

   @Inject
   public void injectDependencies(GlobalConfiguration globalConfiguration, Transport t, InboundInvocationHandler handler,
                                  Marshaller marshaller,
                                  @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e,
                                  CacheManagerNotifier notifier) {
      this.t = t;
      this.t.initialize(globalConfiguration, globalConfiguration.getTransportProperties(), marshaller, e, handler,
                        notifier, globalConfiguration.getDistributedSyncTimeout());
   }

   @Start(priority = 10)
   public void start() {
      t.start();
   }

   @Stop
   public void stop() {
      t.stop();
   }

   public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean stateTransferEnabled) throws Exception {
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

   public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, boolean stateTransferEnabled) throws Exception {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, null, stateTransferEnabled);
   }

   public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean stateTransferEnabled) throws Exception {
      return invokeRemotely(recipients, rpcCommand, mode, timeout, false, null, stateTransferEnabled);
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

   public Transport getTransport() {
      return t;
   }

   public Address getCurrentStateTransferSource() {
      return currentStateTransferSource;
   }

   public Address getLocalAddress() {
      if (t == null) {
         return null;
      }
      return t.getAddress();
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
