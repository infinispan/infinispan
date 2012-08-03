/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.context.Flag.*;

/**
 * State transfer manager.
 * Base class for the distributed and replicated implementations.
 */
public abstract class BaseStateTransferManagerImpl implements StateTransferManager, CacheTopologyHandler {
   private static final Log log = LogFactory.getLog(BaseStateTransferManagerImpl.class);

   private static final boolean trace = log.isTraceEnabled();
   // Injected components
   protected CacheLoaderManager cacheLoaderManager;
   protected Configuration configuration;
   protected RpcManager rpcManager;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected InterceptorChain interceptorChain;
   protected InvocationContextContainer icc;
   protected CacheNotifier cacheNotifier;
   protected StateTransferLock stateTransferLock;
   protected volatile ConsistentHash chOld;
   protected volatile ConsistentHash chNew;
   // closed before the component has been started, open afterwards
   private final CountDownLatch joinStartedLatch = new CountDownLatch(1);
   // closed before the initial state transfer has completed, open afterwards
   private final CountDownLatch joinCompletedLatch = new CountDownLatch(1);
   // closed during state transfer, open the rest of the time
   private final ReclosableLatch stateTransferInProgressLatch = new ReclosableLatch(false);
   private volatile BaseStateTransferTask stateTransferTask;
   private CommandBuilder commandBuilder;
   protected TransactionTable transactionTable;
   private LockContainer<?> lockContainer;
   private String cacheName;
   private GlobalConfiguration globalCfg;
   protected boolean withTopology;

   private LocalTopologyManager localTopologyManager;

   public BaseStateTransferManagerImpl() {
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory cf,
                    DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                    CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier, StateTransferLock stateTransferLock,
                    TransactionTable transactionTable, LockContainer<?> lockContainer, Cache cache,
                    GlobalConfiguration globalCfg, LocalTopologyManager localTopologyManager) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.cf = cf;
      this.stateTransferLock = stateTransferLock;
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.cacheNotifier = cacheNotifier;
      this.transactionTable = transactionTable;
      this.lockContainer = lockContainer;
      this.cacheName = cache.getName();
      this.globalCfg = globalCfg;

      this.localTopologyManager = localTopologyManager;
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   private void start() throws Exception {
      if (configuration.transaction().transactionMode().isTransactional() &&
            configuration.versioning().enabled() &&
            configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            configuration.clustering().cacheMode().isClustered()) {

         // We need to use a special form of PutKeyValueCommand that can apply versions too.
         commandBuilder = new CommandBuilder() {
            @Override
            public PutKeyValueCommand buildPut(InvocationContext ctx, CacheEntry e) {
               return cf.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), ctx.getFlags());
            }
         };
      } else {
         commandBuilder = new CommandBuilder() {
            @Override
            public PutKeyValueCommand buildPut(InvocationContext ctx, CacheEntry e) {
               return cf.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), ctx.getFlags());
            }
         };
      }

      this.withTopology = globalCfg.transport().hasTopologyInfo();

      if (trace) log.tracef("Starting state transfer manager on " + getAddress());

      // set up the old CH, but it shouldn't be used until we get the prepare call
      //cacheViewsManager.join(cacheName, this);
      HashConfiguration hashConfig = configuration.clustering().hash();
      Hash hashFunction = hashConfig.hash();
      int numSegments = hashConfig.numVirtualNodes();
      int numOwners = hashConfig.numOwners();
      long timeout = configuration.clustering().stateTransfer().timeout();
      localTopologyManager.join(cacheName, new CacheJoinInfo(new DefaultConsistentHashFactory(),
            hashFunction, numSegments, numOwners, timeout), this);
   }

   protected abstract ConsistentHash createConsistentHash(List<Address> members);

   // To avoid blocking other components' start process, wait last, if necessary, for join to complete.
   @Override
   @Start(priority = 1000)
   public void waitForJoinToComplete() throws InterruptedException {
      joinCompletedLatch.await(getTimeout(), TimeUnit.MILLISECONDS);
   }

   @Stop(priority = 20)
   public void stop() {
      /*
      chOld = null;
      chNew = null;
      // cancel any pending state transfer before leaving
      BaseStateTransferTask tempTask = stateTransferTask;
      if (tempTask != null) {
         tempTask.cancelStateTransfer(true);
         stateTransferTask = null;
      }
      cacheViewsManager.leave(cacheName);
      joinStartedLatch.countDown();
      joinCompletedLatch.countDown();
      stateTransferInProgressLatch.open();
      */
   }

   protected Address getAddress() {
      return rpcManager.getAddress();
   }

   @Override
   public boolean hasJoinStarted() {
      return isLatchOpen(joinStartedLatch);
   }

   @Override
   public void waitForJoinToStart() throws InterruptedException {
      joinStartedLatch.await(getTimeout(), TimeUnit.MILLISECONDS);
   }

   @Override
   public boolean isJoinComplete() {
      return isLatchOpen(joinCompletedLatch);
   }

   @Override
   public boolean isStateTransferInProgress() {
      return !isLatchOpen(stateTransferInProgressLatch);
   }

   public void waitForStateTransferToStart(int viewId) throws InterruptedException {
      // TODO Add another latch for this, or maybe use a lock with condition variables instead
//      while ((newView == null || newView.getViewId() < viewId)
//            && (oldView == null || oldView.getViewId() < viewId)) {
//         Thread.sleep(10);
//      }
   }

   @Override
   public void waitForStateTransferToComplete() throws InterruptedException {
      stateTransferInProgressLatch.await(getTimeout(), TimeUnit.MILLISECONDS);
   }

   private boolean isLatchOpen(CountDownLatch latch) {
         return latch.getCount() == 0;
   }

   private boolean isLatchOpen(ReclosableLatch latch) {
        return latch.isOpened();
   }

   @Override
   public void applyState(Collection<InternalCacheEntry> state,
                          Address sender, int viewId) throws InterruptedException {
      waitForStateTransferToStart(viewId);
//      if (newView == oldView) {
//         log.remoteStateRejected(sender, viewId, oldView.getViewId());
//         return;
//      }
//      if (viewId != newView.getViewId()) {
//         log.debugf("Rejecting state pushed by node %s for rehash %d (last view id we know is %d)", sender, viewId, newView.getViewId());
//         return;
//      }

      if (state != null) {
         log.debugf("Applying new state from %s: received %d keys", sender, state.size());
         if (trace) log.tracef("Received keys: %s", keys(state));
         for (InternalCacheEntry e : state) {
            InvocationContext ctx = icc.createInvocationContext(false, 1);
            // locking not necessary as during rehashing we block all transactions
            ctx.setFlags(CACHE_MODE_LOCAL, IGNORE_RETURN_VALUES, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING,
                         SKIP_OWNERSHIP_CHECK);
            try {
               PutKeyValueCommand put = commandBuilder.buildPut(ctx, e);
               interceptorChain.invoke(ctx, put);
            } catch (Exception ee) {
               log.problemApplyingStateForKey(ee.getMessage(), e.getKey());
            }
         }

         if(trace) log.tracef("After applying state data container has %d keys", dataContainer.size());
      }
   }

   @Override
   public void applyLocks(Collection<LockInfo> lockInfo, Address sender, int viewId) throws InterruptedException {
      if (lockInfo != null) {
         log.debugf("Integrating %d locks from %s", lockInfo.size(), sender);
         for (LockInfo lock : lockInfo) {
            RemoteTransaction remoteTx = transactionTable.getRemoteTransaction(lock.getGlobalTransaction());
            if (remoteTx == null) {
               remoteTx = transactionTable.createRemoteTransaction(lock.getGlobalTransaction(), null);
               remoteTx.setMissingModifications(true);
            }
            Object result = lockContainer.acquireLock(remoteTx.getGlobalTransaction(), lock.getKey(), 0, TimeUnit.SECONDS);
            if (result == null) {
               throw new IllegalStateException("Could not acquire lock for key " + lock.getKey());
            }
         }
      }
   }

   @Override
   public boolean isLocationInDoubt(Object key) {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
   }

   private Collection<Object> keys(Collection<InternalCacheEntry> state) {
      Collection<Object> result = new ArrayList<Object>(state.size());
      for (InternalCacheEntry e : state) {
         result.add(e.getKey());
      }
      return result;
   }

   // package-protected methods used by StateTransferTask

   /**
    * @return <code>true</code> if the state transfer started successfully, <code>false</code> otherwise
    */
   public boolean startStateTransfer(int viewId, Collection<Address> members, boolean initialView) throws TimeoutException, InterruptedException, StateTransferCancelledException {
//      if (newView == null || viewId != newView.getViewId()) {
//         log.debugf("Cannot start state transfer for view %d, we should be starting state transfer for view %s", viewId, newView);
//         return false;
//      }
      stateTransferInProgressLatch.close();
      return true;
   }

   public abstract CacheStore getCacheStoreForStateTransfer();

   public void pushStateToNode(NotifyingNotifiableFuture<Object> stateTransferFuture, int viewId, Collection<Address> targets,
                               Collection<InternalCacheEntry> state, Collection<LockInfo> lockInfo) throws StateTransferCancelledException {
      StateTransferControlCommand.Type type;
      if (state != null) {
         log.debugf("Pushing to nodes %s %d keys", targets,  state.size());
         log.tracef("Pushing to nodes %s keys: %s", targets, keys(state));
         type =  StateTransferControlCommand.Type.APPLY_STATE;
      } else  if (lockInfo != null) {
         log.debugf("Migrating %d locks to node(s) %s", lockInfo.size(), targets);
         type =  StateTransferControlCommand.Type.APPLY_LOCKS;
      } else {
         throw new IllegalStateException("Cannot have both locks and state set to null.");
      }

      final StateTransferControlCommand cmd = cf.buildStateTransferCommand(type, getAddress(), viewId, state, lockInfo);

      rpcManager.invokeRemotelyInFuture(targets, cmd, false, stateTransferFuture, getTimeout());
   }

//   public boolean isLastViewId(int viewId) {
//      return viewId == newView.getViewId();
//   }
//
//   @Override
//   public void prepareView(CacheView pendingView, CacheView committedView) throws Exception {
//      log.tracef("Received new cache view: %s %s", cacheName, pendingView);
//
//      joinStartedLatch.countDown();
//
//      // if this is the first view we're seeing, initialize the oldView as well
//      if (oldView == null) {
//         oldView = committedView;
//      }
//      newView = pendingView;
//      chNew = createConsistentHash(pendingView.getMembers());
//
//      stateTransferTask = createStateTransferTask(pendingView.getViewId(), pendingView.getMembers(), chOld == null);
//      stateTransferTask.performStateTransfer();
//   }
//
//   @Override
//   public void commitView(int viewId) {
//      BaseStateTransferTask tempTask = stateTransferTask;
//      if (tempTask == null) {
//         if (viewId == oldView.getViewId()) {
//            log.tracef("Ignoring commit for cache view %d as we have already committed it", viewId);
//            return;
//         } else {
//            throw new IllegalArgumentException(String.format("Cannot commit view %d, we are at view %d",
//                  viewId, oldView.getViewId()));
//         }
//      }
//
//      tempTask.commitStateTransfer();
//      stateTransferTask = null;
//
//      // we can now use the new CH as the baseline for the next rehash
//      oldView = newView;
//      chOld = chNew;
//   }
//
//   @Override
//   public void rollbackView(int newViewId, int committedViewId) {
//      BaseStateTransferTask tempTask = stateTransferTask;
//      if (tempTask == null) {
//         if (committedViewId == oldView.getViewId()) {
//            log.tracef("Ignoring rollback for cache view %d as we don't have a state transfer in progress",
//                  committedViewId);
//            return;
//         } else {
//            throw new IllegalArgumentException(String.format("Cannot rollback to view %d, we are at view %d",
//                  committedViewId, oldView.getViewId()));
//         }
//      }
//
//      tempTask.cancelStateTransfer(true);
//      stateTransferTask = null;
//
//      newView = new CacheView(newViewId, oldView.getMembers());
//      oldView = newView;
//      chNew = chOld;
//
//      stateTransferInProgressLatch.open();
//      joinCompletedLatch.countDown();
//   }
//
//   @Override
//   public void preInstallView() {
//      stateTransferLock.blockNewTransactionsAsync();
//   }
//
//   @Override
//   public void postInstallView(int viewId) {
//      try {
//         stateTransferLock.unblockNewTransactions(viewId);
//      } catch (Exception e) {
//         log.errorUnblockingTransactions(e);
//      }
//
//      stateTransferInProgressLatch.open();
//      // getCache() will only return after joining has completed, so we need that to be last
//      joinCompletedLatch.countDown();
//   }

   protected abstract BaseStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView);


   @Override
   public void updateConsistentHash(int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) {
      if (pendingCH == null && currentCH.getMembers().contains(rpcManager.getAddress())) {
         joinCompletedLatch.countDown();
      }
   }

   @Override
   public void rebalance(int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) {
   }

   private static interface CommandBuilder {
      PutKeyValueCommand buildPut(InvocationContext ctx, CacheEntry e);
   }

   protected abstract long getTimeout();
}
