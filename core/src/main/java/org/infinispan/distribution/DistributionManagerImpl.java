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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.context.Flag.*;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {
   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // Injected components
   private CacheLoaderManager cacheLoaderManager;
   private Configuration configuration;
   private RpcManager rpcManager;
   private CacheManagerNotifier notifier;
   private CommandsFactory cf;
   private TransactionLogger transactionLogger;
   private DataContainer dataContainer;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private CacheNotifier cacheNotifier;

   private final ViewChangeListener listener;
   private final ExecutorService rehashExecutor;

   // consistentHash and self are not valid in the inbound threads until
   // joinStartedLatch has been signaled by the starting thread
   // we don't have a getSelf() that waits on joinS
   private volatile ConsistentHash consistentHash;
   // the previous consistent hash is only updated when a rehash has finished in the entire cluster
   private volatile ConsistentHash lastSuccessfulCH;
   private Address self;
   private final CountDownLatch joinStartedLatch = new CountDownLatch(1);

   /**
    * Set if the cluster is in rehash mode, i.e. not all the nodes have applied the new state.
    */
   private volatile boolean rehashInProgress = false;
   /**
    * When we get the rehash completed notification we have to first invalidate the moved keys
    * Only then we can unset the rehashInProgress flag.
    */
   private volatile boolean receivedRehashCompletedNotification = false;
   private final Object rehashInProgressMonitor = new Object();

   private volatile int lastViewId = -1;

   // these fields are only used on the coordinator
   private int lastViewIdFromPushConfirmation = -1;
   private final Map<Address, Integer> pushConfirmations = new HashMap<Address, Integer>(1);

   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.")
   @Metric(displayName = "Is join completed?", dataType = DataType.TRAIT)
   private volatile boolean joinComplete = false;
   private final CountDownLatch joinCompletedLatch = new CountDownLatch(1);

   /**
    * Default constructor
    */
   public DistributionManagerImpl() {
      super();
      LinkedBlockingQueue<Runnable> rehashQueue = new LinkedBlockingQueue<Runnable>(1);
      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("Rehasher," + configuration.getName()
                  + "," + rpcManager.getTransport().getAddress());
            return t;
         }
      };
      rehashExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, rehashQueue, tf,
                                              new ThreadPoolExecutor.DiscardOldestPolicy());
      listener = new ViewChangeListener();
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier, CommandsFactory cf,
                    DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                    CacheLoaderManager cacheLoaderManager, InboundInvocationHandler inboundInvocationHandler,
                    CacheNotifier cacheNotifier) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
      this.cf = cf;
      this.transactionLogger = new TransactionLoggerImpl(cf, configuration);
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.cacheNotifier = cacheNotifier;
   }

   // needs to be AFTER the RpcManager
   // The DMI is cache-scoped, so it will always start after the RMI, which is global-scoped
   @Start(priority = 20)
   private void join() throws Exception {
      if (trace) log.trace("starting distribution manager on " + getMyAddress());
      notifier.addListener(listener);

      Transport t = rpcManager.getTransport();
      List<Address> members = t.getMembers();
      self = t.getAddress();
      lastViewId = t.getViewId();
      consistentHash = ConsistentHashHelper.createConsistentHash(configuration, members);
      lastSuccessfulCH = ConsistentHashHelper.createConsistentHash(configuration, members);

      // in case we are/become the coordinator, make sure we're in the push confirmations map before anyone else
      synchronized (pushConfirmations) {
         pushConfirmations.put(t.getAddress(), -1);
      }

      // allow incoming requests
      joinStartedLatch.countDown();

      // nothing to push, but we need to inform the coordinator that we have finished our push
      notifyCoordinatorPushCompleted(t.getViewId());
   }

   private int getReplCount() {
      return configuration.getNumOwners();
   }

   private Address getMyAddress() {
      return rpcManager != null ? rpcManager.getAddress() : null;
   }

   public RpcManager getRpcManager() {
      return rpcManager;
   }

   // To avoid blocking other components' start process, wait last, if necessary, for join to complete.

   @Override
   @Start(priority = 1000)
   public void waitForJoinToComplete() throws InterruptedException {
      joinCompletedLatch.await();
      joinComplete = true;
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
      synchronized (rehashInProgressMonitor) {
         rehashInProgressMonitor.notifyAll();
      }
      rehashExecutor.shutdownNow();
      joinStartedLatch.countDown();
      joinCompletedLatch.countDown();
      joinComplete = true;
   }


   @Deprecated
   public boolean isLocal(Object key) {
      return getLocality(key).isLocal();
   }

   public DataLocality getLocality(Object key) {
      boolean local = getConsistentHash().isKeyLocalToAddress(getSelf(), key, getReplCount());
      if (isRehashInProgress()) {
         if (local) {
            return DataLocality.LOCAL_UNCERTAIN;
         } else {
            return DataLocality.NOT_LOCAL_UNCERTAIN;
         }
      } else {
         if (local) {
            return DataLocality.LOCAL;
         } else {
            return DataLocality.NOT_LOCAL;
         }
      }
   }


   public List<Address> locate(Object key) {
      return getConsistentHash().locate(key, getReplCount());
   }

   public boolean waitForRehashToComplete(int viewId) throws InterruptedException, TimeoutException {
      long endTime = System.currentTimeMillis() + configuration.getRehashRpcTimeout();
      synchronized (rehashInProgressMonitor) {
         while (!receivedRehashCompletedNotification && lastViewId == viewId && System.currentTimeMillis() < endTime) {
            rehashInProgressMonitor.wait(configuration.getRehashRpcTimeout());
         }
      }
      if (!receivedRehashCompletedNotification) {
         if (lastViewId != viewId) {
            log.debug("Received a new view while waiting for cluster-wide rehash to finish");
            return false;
         } else {
            throw new TimeoutException("Timeout waiting for cluster-wide rehash to finish");
         }
      } else {
         log.debug("Cluster-wide rehash finished successfully.");
      }
      return true;
   }

   /**
    * Hold up operations on incoming threads until the starting thread has finished initializing the consistent hash
    */
   private void waitForJoinToStart() throws InterruptedException {
      joinStartedLatch.await();
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
      return locateAll(keys, getReplCount());
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys, int numOwners) {
      return getConsistentHash().locateAll(keys, numOwners);
   }

   public void transformForL1(CacheEntry entry) {
      if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.getL1Lifespan())
         entry.setLifespan(configuration.getL1Lifespan());
   }

   public InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx) throws Exception {
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, ctx.getFlags());

      List<Address> targets = locate(key);
      targets.remove(getSelf());
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets);
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.SYNCHRONOUS,
                                                                   configuration.getSyncReplTimeout(), false, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
               return cacheValue.toInternalCacheEntry(key);
            }
         }
      }

      return null;
   }

   public Address getSelf() {
      return self;
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public ConsistentHash setConsistentHash(ConsistentHash consistentHash) {
      if (trace) log.tracef("Installing new consistent hash %s", consistentHash);
      cacheNotifier.notifyTopologyChanged(lastSuccessfulCH, consistentHash, true);
      this.consistentHash = consistentHash;
      cacheNotifier.notifyTopologyChanged(lastSuccessfulCH, consistentHash, false);
      return lastSuccessfulCH;
   }


   @ManagedOperation(description = "Determines whether a given key is affected by an ongoing rehash, if any.")
   @Operation(displayName = "Could key be affected by rehash?")
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      // TODO Do we really need to check if it's local now or is it enough to check that it wasn't local in the last CH
      return isRehashInProgress() && consistentHash.locate(key, getReplCount()).contains(getSelf())
            && !lastSuccessfulCH.locate(key, getReplCount()).contains(getSelf());
   }

   public TransactionLogger getTransactionLogger() {
      return transactionLogger;
   }

   private Map<Object, InternalCacheValue> applyStateMap(Map<Object, InternalCacheValue> state, boolean withRetry) {
      Map<Object, InternalCacheValue> retry = withRetry ? new HashMap<Object, InternalCacheValue>() : null;

      for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
         InternalCacheValue v = e.getValue();
         NonTxInvocationContext ctx = (NonTxInvocationContext) icc.createInvocationContext(false);
         ctx.setOriginLocal(false);
         // locking not necessary in the case of a join since the node isn't doing anything else
         // TODO what if the node is already running?
         ctx.setFlags(CACHE_MODE_LOCAL, SKIP_CACHE_LOAD, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING,
                      SKIP_OWNERSHIP_CHECK);
         try {
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle(), ctx.getFlags());
            interceptorChain.invoke(ctx, put);
         } catch (Exception ee) {
            if (withRetry) {
               if (trace)
                  log.tracef("Problem %s encountered when applying state for key %s. Adding entry to retry queue.", ee.getMessage(), e.getKey());
               retry.put(e.getKey(), e.getValue());
            } else {
               log.problemApplyingStateForKey(ee.getMessage(), e.getKey());
            }
         }
      }
      return retry;
   }

   @Override
   public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state,
                          Address sender, int viewId) throws InterruptedException {
      waitForJoinToStart();

      if (viewId < lastViewId) {
         log.debugf("Rejecting state pushed by node %s for old rehash %d (last view id is %d)", sender, viewId, lastViewId);
         return;
      }

      log.debugf("Applying new state from %s: received %d keys", sender, state.size());
      if (trace) log.tracef("Received keys: %s", state.keySet());
      int retryCount = 3; // in case we have issues applying state.
      Map<Object, InternalCacheValue> pendingApplications = state;
      for (int i = 0; i < retryCount; i++) {
         pendingApplications = applyStateMap(pendingApplications, true);
         if (pendingApplications.isEmpty()) break;
      }
      // one last go
      if (!pendingApplications.isEmpty()) applyStateMap(pendingApplications, false);

      if(trace) log.tracef("After applying state data container has %d keys", dataContainer.size());
   }

   @Override
   public void markRehashCompleted(int viewId) throws InterruptedException {
      waitForJoinToStart();

      if (viewId < lastViewId) {
         if (trace)
            log.tracef("Ignoring old rehash completed confirmation for view %d, last view is %d", viewId, lastViewId);
         return;
      }

      if (viewId > lastViewId) {
         throw new IllegalStateException("Received rehash completed confirmation before confirming it ourselves");
      }

      if (trace) log.tracef("Rehash completed on node %s, data container has %d keys", getSelf(), dataContainer.size());
      receivedRehashCompletedNotification = true;
      synchronized (rehashInProgressMonitor) {
         // we know for sure the rehash task is waiting for this confirmation, so the CH hasn't been replaced
         if (trace) log.tracef("Updating last rehashed CH to %s", this.lastSuccessfulCH);
         lastSuccessfulCH = this.consistentHash;
         rehashInProgressMonitor.notifyAll();
      }
      joinCompletedLatch.countDown();
   }

   @Override
   public void markNodePushCompleted(int viewId, Address node) throws InterruptedException {
      waitForJoinToStart();

      if (trace)
         log.tracef("Coordinator: received push completed notification for %s, view id %s", node, viewId);

      // ignore all push confirmations for view ids smaller than our view id
      if (viewId < lastViewId) {
         if (log.isTraceEnabled())
            log.tracef("Coordinator: Ignoring old push completed confirmation for view %d, last view is %d", viewId, lastViewId);
         return;
      }

      synchronized (pushConfirmations) {
         if (viewId < lastViewIdFromPushConfirmation) {
            if (trace)
               log.tracef("Coordinator: Ignoring old push completed confirmation for view %d, last confirmed view is %d", viewId, lastViewIdFromPushConfirmation);
            return;
         }

         // update the latest received view id if necessary
         if (viewId > lastViewIdFromPushConfirmation) {
            lastViewIdFromPushConfirmation = viewId;
         }

         pushConfirmations.put(node, viewId);
         if (trace)
            log.tracef("Coordinator: updated push confirmations map %s", pushConfirmations);

         // the view change listener ensures that all the member nodes have an entry in the map
         for (Map.Entry<Address, Integer> pushNode : pushConfirmations.entrySet()) {
            if (pushNode.getValue() < viewId) {
               return;
            }
         }

         if (trace)
            log.tracef("Coordinator: sending rehash completed notification for view %d, lastView %d, notifications received: %s", viewId, lastViewId, pushConfirmations);

         // all the nodes are up-to-date, broadcast the rehash completed command
         final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.REHASH_COMPLETED, getSelf(), viewId);

         // all nodes will eventually receive the command, no need to wait here
         rpcManager.broadcastRpcCommand(cmd, false);
         // The broadcast doesn't send the message to the local node
         markRehashCompleted(viewId);
      }
   }

   public void notifyCoordinatorPushCompleted(int viewId) throws Exception {
      Transport t = rpcManager.getTransport();

      if (t.isCoordinator()) {
         if (trace) log.tracef("Node %s is the coordinator, marking push for %d as complete directly", self, viewId);
         markNodePushCompleted(viewId, self);
      } else {
         final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.NODE_PUSH_COMPLETED, self, viewId);
         Address coordinator = rpcManager.getTransport().getCoordinator();

         if (trace) log.tracef("Node %s is not the coordinator, sending request to mark push for %d as complete to %s", self, viewId, coordinator);
         rpcManager.invokeRemotely(Collections.singleton(coordinator), cmd, ResponseMode.SYNCHRONOUS, configuration.getRehashRpcTimeout());
      }
   }

   @Listener
   public class ViewChangeListener {
      @Merged @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         if(trace)
            log.tracef("New view received: %d, type=%s, members: %s. Starting the RebalanceTask", e.getViewId(), e.getType(), e.getNewMembers());

         synchronized (rehashInProgressMonitor) {
            rehashInProgress = true;
            receivedRehashCompletedNotification = false;
            lastViewId = e.getViewId();
            rehashInProgressMonitor.notifyAll();
         }

         // make sure the pushConfirmations map has one entry for each cluster member
         // we will always have
         if (DistributionManagerImpl.this.getRpcManager().getTransport().isCoordinator()) {
            synchronized (pushConfirmations) {
               for (Address newNode : e.getNewMembers()) {
                  if (!pushConfirmations.containsKey(newNode)) {
                     if (trace)
                        log.tracef("Coordinator: adding new node %s", newNode);
                     pushConfirmations.put(newNode, -1);
                  }
               }
               for (Address oldNode : e.getOldMembers()) {
                  if (!e.getNewMembers().contains(oldNode)) {
                     if (trace)
                        log.tracef("Coordinator: removing node %s", oldNode);
                     pushConfirmations.remove(oldNode);
                  }
               }
               if (trace)
                  log.tracef("Coordinator: push confirmations list updated: %s", pushConfirmations);
            }
         }

         RebalanceTask rebalanceTask = new RebalanceTask(rpcManager, cf, configuration, dataContainer,
               DistributionManagerImpl.this, icc, cacheNotifier, interceptorChain, e.getViewId());
         rehashExecutor.submit(rebalanceTask);
      }
   }

   public CacheStore getCacheStoreForRehashing() {
      if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared())
         return null;
      return cacheLoaderManager.getCacheStore();
   }

   @ManagedAttribute(description = "Checks whether there is a pending rehash in the cluster.")
   @Metric(displayName = "Is rehash in progress?", dataType = DataType.TRAIT)
   public boolean isRehashInProgress() {
      return rehashInProgress;
   }

   @Override
   public void markRehashTaskCompleted() {
      synchronized (rehashInProgressMonitor) {
         rehashInProgress = false;
         rehashInProgressMonitor.notifyAll();
      }
   }

   public boolean isJoinComplete() {
      return joinComplete;
   }

   public Collection<Address> getAffectedNodes(Collection<Object> affectedKeys) {
      if (affectedKeys == null || affectedKeys.isEmpty()) {
         if (trace) log.trace("affected keys are empty");
         return Collections.emptyList();
      }

      Set<Address> an = new HashSet<Address>();
      for (List<Address> addresses : locateAll(affectedKeys).values()) an.addAll(addresses);
      return an;
   }

   public void applyRemoteTxLog(List<WriteCommand> commands) {
      for (WriteCommand cmd : commands) {
         try {
            // this is a remotely originating tx
            cf.initializeReplicableCommand(cmd, true);
            InvocationContext ctx = icc.createInvocationContext(true);
            ctx.setFlags(SKIP_REMOTE_LOOKUP, CACHE_MODE_LOCAL, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING);
            interceptorChain.invoke(ctx, cmd);
         } catch (Exception e) {
            log.exceptionWhenReplaying(cmd, e);
         }
      }

   }

   @ManagedOperation(description = "Tells you whether a given key is local to this instance of the cache.  Only works with String keys.")
   @Operation(displayName = "Is key local?")
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return getLocality(key).isLocal();
   }

   @ManagedOperation(description = "Locates an object in a cluster.  Only works with String keys.")
   @Operation(displayName = "Locate key")
   public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
      List<String> l = new LinkedList<String>();
      for (Address a : locate(key)) l.add(a.toString());
      return l;
   }

   @Override
   public String toString() {
      return "DistributionManagerImpl[rehashInProgress=" + rehashInProgress + ", consistentHash=" + consistentHash + "]";
   }

   public void setConfiguration(Configuration configuration) {
      this.configuration = configuration;
   }
}
