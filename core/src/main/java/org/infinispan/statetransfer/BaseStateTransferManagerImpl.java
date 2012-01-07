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

import org.infinispan.cacheviews.CacheView;
import org.infinispan.cacheviews.CacheViewListener;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.ReclosableLatch;
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
public abstract class BaseStateTransferManagerImpl implements StateTransferManager, CacheViewListener {
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
   private CacheViewsManager cacheViewsManager;
   protected StateTransferLock stateTransferLock;
   protected volatile ConsistentHash chOld;
   private volatile CacheView oldView;
   protected volatile ConsistentHash chNew;
   private volatile CacheView newView;
   // closed before the component has been started, open afterwards
   private final CountDownLatch joinStartedLatch = new CountDownLatch(1);
   // closed before the initial state transfer has completed, open afterwards
   private final CountDownLatch joinCompletedLatch = new CountDownLatch(1);
   // closed during state transfer, open the rest of the time
   private final ReclosableLatch stateTransferInProgressLatch = new ReclosableLatch(false);
   private volatile BaseStateTransferTask stateTransferTask;
   private CommandBuilder commandBuilder;

   public BaseStateTransferManagerImpl() {
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory cf,
                    DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                    CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier, StateTransferLock stateTransferLock,
                    CacheViewsManager cacheViewsManager) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.cf = cf;
      this.stateTransferLock = stateTransferLock;
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.cacheNotifier = cacheNotifier;
      this.cacheViewsManager = cacheViewsManager;
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   private void start() throws Exception {
      if (configuration.isTransactionalCache() &&
            configuration.isEnableVersioning() &&
            configuration.isWriteSkewCheck() &&
            configuration.getTransactionLockingMode() == LockingMode.OPTIMISTIC &&
            configuration.getCacheMode().isClustered()) {

         // We need to use a special form of PutKeyValueCommand that can apply versions too.
         commandBuilder = new CommandBuilder() {
            @Override
            public PutKeyValueCommand buildPut(InvocationContext ctx, CacheEntry e) {
               EntryVersion version = e.getVersion();
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

      if (trace) log.tracef("Starting state transfer manager on " + getAddress());

      // set up the old CH, but it shouldn't be used until we get the prepare call
      cacheViewsManager.join(configuration.getName(), this);
   }

   protected abstract ConsistentHash createConsistentHash(List<Address> members);

   // To avoid blocking other components' start process, wait last, if necessary, for join to complete.
   @Start(priority = 1000)
   public void waitForJoinToComplete() throws InterruptedException {
      joinCompletedLatch.await(configuration.getRehashWaitTime(), TimeUnit.MILLISECONDS);
   }

   @Stop(priority = 20)
   public void stop() {
      chOld = null;
      chNew = null;
      // cancel any pending state transfer before leaving
      BaseStateTransferTask tempTask = stateTransferTask;
      if (tempTask != null) {
         tempTask.cancelStateTransfer(true, false);
         stateTransferTask = null;
      }
      cacheViewsManager.leave(configuration.getName());
      joinStartedLatch.countDown();
      joinCompletedLatch.countDown();
      stateTransferInProgressLatch.open();
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
      joinStartedLatch.await(configuration.getRehashWaitTime(), TimeUnit.MILLISECONDS);
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
      while (newView == null || newView.getViewId() < viewId) {
         Thread.sleep(1);
      }
   }

   @Override
   public void waitForStateTransferToComplete() throws InterruptedException {
      stateTransferInProgressLatch.await(configuration.getRehashWaitTime(), TimeUnit.MILLISECONDS);
   }

   private boolean isLatchOpen(CountDownLatch latch) {
      try {
         return latch.await(0, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return true;
      }
   }

   private boolean isLatchOpen(ReclosableLatch latch) {
      try {
         return latch.await(0, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         return true;
      }
   }

   @Override
   public void applyState(Collection<InternalCacheEntry> state,
                          Address sender, int viewId) throws InterruptedException {
      waitForStateTransferToStart(viewId);
      if (newView == oldView) {
         log.remoteStateRejected(sender, viewId, oldView.getViewId());
         return;
      }
      if (viewId != newView.getViewId()) {
         log.debugf("Rejecting state pushed by node %s for rehash %d (last view id we know is %d)", sender, viewId, newView.getViewId());
         return;
      }

      log.debugf("Applying new state from %s: received %d keys", sender, state.size());
      if (trace) log.tracef("Received keys: %s", keys(state));
      for (InternalCacheEntry e : state) {
         InvocationContext ctx = icc.createInvocationContext(false, 1);
         // locking not necessary as during rehashing we block all transactions
         ctx.setFlags(CACHE_MODE_LOCAL, SKIP_CACHE_LOAD, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING,
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
      if (newView == null || viewId != newView.getViewId()) {
         log.debugf("Cannot start state transfer for view %d, we should be starting state transfer for view %s", viewId, newView);
         return false;
      }
      stateTransferInProgressLatch.close();
      return true;
   }

   public void endStateTransfer() {
      // we can now use the new CH as the baseline for the next rehash
      oldView = newView;
      chOld = chNew;

      stateTransferInProgressLatch.open();
      joinCompletedLatch.countDown();
   }

   public abstract CacheStore getCacheStoreForStateTransfer();

   public void pushStateToNode(NotifyingNotifiableFuture<Object> stateTransferFuture, int viewId, Collection<Address> targets,
                               Collection<InternalCacheEntry> state) throws StateTransferCancelledException {
      log.debugf("Pushing to nodes %s %d keys", targets, state.size());
      log.tracef("Pushing to nodes %s keys: %s", targets, keys(state));

      final StateTransferControlCommand cmd = cf.buildStateTransferCommand(StateTransferControlCommand.Type.APPLY_STATE, getAddress(), viewId, state);

      rpcManager.invokeRemotelyInFuture(targets, cmd, false, stateTransferFuture, configuration.getRehashRpcTimeout());
   }

   public boolean isLastViewId(int viewId) {
      return viewId == newView.getViewId();
   }

   @Override
   public void prepareView(CacheView pendingView, CacheView committedView) throws Exception {
      log.tracef("Received new cache view: %s %s", configuration.getName(), pendingView);

      joinStartedLatch.countDown();

      newView = pendingView;
      chNew = createConsistentHash(pendingView.getMembers());

      stateTransferTask = createStateTransferTask(pendingView.getViewId(), pendingView.getMembers(), chOld == null);
      stateTransferTask.performStateTransfer();
   }

   @Override
   public void commitView(int viewId) {
      BaseStateTransferTask tempTask = stateTransferTask;
      if (tempTask == null) {
         if (viewId == oldView.getViewId()) {
            log.tracef("Ignoring commit for cache view %d as we have already committed it", viewId);
            return;
         } else {
            throw new IllegalArgumentException(String.format("Cannot commit view %d, we are at view %d",
                  viewId, oldView.getViewId()));
         }
      }

      tempTask.commitStateTransfer();
      stateTransferTask = null;
      endStateTransfer();
   }

   @Override
   public void rollbackView(int committedViewId) {
      BaseStateTransferTask tempTask = stateTransferTask;
      if (tempTask == null) {
         if (committedViewId == oldView.getViewId()) {
            log.tracef("Ignoring rollback for cache view %d as we don't have a state transfer in progress",
                  committedViewId);
            return;
         } else {
            throw new IllegalArgumentException(String.format("Cannot rollback to view %d, we are at view %d",
                  committedViewId, oldView.getViewId()));
         }
      }

      tempTask.cancelStateTransfer(true, false);
      stateTransferTask = null;

      // TODO Use the new view id
      newView = oldView;
      chNew = chOld;

      stateTransferInProgressLatch.open();
      joinCompletedLatch.countDown();
   }

   @Override
   public void waitForPrepare() {
      stateTransferLock.blockNewTransactionsAsync();
   }

   protected abstract BaseStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView);

   private static interface CommandBuilder {
      PutKeyValueCommand buildPut(InvocationContext ctx, CacheEntry e);
   }
}
