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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.context.Flag.*;

/**
 * State transfer manager.
 * Base class for the distributed and replicated implementations.
 */
public abstract class BaseStateTransferManagerImpl implements StateTransferManager {
   private static final Log log = LogFactory.getLog(BaseStateTransferManagerImpl.class);

   private static final boolean trace = log.isTraceEnabled();
   // Injected components
   protected CacheLoaderManager cacheLoaderManager;
   protected Configuration configuration;
   protected RpcManager rpcManager;
   private CacheManagerNotifier notifier;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected InterceptorChain interceptorChain;
   protected InvocationContextContainer icc;
   protected CacheNotifier cacheNotifier;
   protected StateTransferLock stateTransferLock;
   protected final ViewChangeListener listener;
   protected final ExecutorService rehashExecutor;
   protected final PushConfirmationsMap pushConfirmations;
   protected volatile ConsistentHash chOld;
   private volatile int oldViewId;
   protected volatile ConsistentHash chNew;
   private volatile int newViewId;
   // closed before the component has been started, open afterwards
   private final CountDownLatch joinStartedLatch = new CountDownLatch(1);
   // closed before the initial state transfer has completed, open afterwards
   private final CountDownLatch joinCompletedLatch = new CountDownLatch(1);
   // closed during state transfer, open the rest of the time
   private final ReclosableLatch stateTransferInProgressLatch = new ReclosableLatch(false);

   public BaseStateTransferManagerImpl() {
      listener = new ViewChangeListener();
      pushConfirmations = new PushConfirmationsMap();

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
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier, CommandsFactory cf,
                    DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                    CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier, StateTransferLock stateTransferLock) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
      this.cf = cf;
      this.stateTransferLock = stateTransferLock;
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.cacheNotifier = cacheNotifier;
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 21)
   private void start() throws Exception {
      if (trace) log.tracef("Starting distribution manager on " + getAddress());
      notifier.addListener(listener);

      // set up the old CH so that the rebalance task can start and wait for the
      Transport transport = rpcManager.getTransport();
      oldViewId = transport.getViewId();
      List<Address> members = transport.getMembers();
      chOld = createConsistentHash(members);

      newViewReceived(oldViewId, members, true, false);

      joinStartedLatch.countDown();
   }

   protected abstract ConsistentHash createConsistentHash(List<Address> members);

   // To avoid blocking other components' start process, wait last, if necessary, for join to complete.
   @Start(priority = 1000)
   public void waitForJoinToComplete() throws InterruptedException {
      joinCompletedLatch.await(configuration.getRehashWaitTime(), TimeUnit.MILLISECONDS);
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
      rehashExecutor.shutdownNow();
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
      while (isLatchOpen(stateTransferInProgressLatch) && newViewId < viewId) {
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
   public void nodeCompletedPush(Address sender, int viewId) {
      pushConfirmations.confirmPush(sender, viewId);
   }

   @Override
   public void requestJoin(Address sender, int viewId) {
      pushConfirmations.confirmJoin(sender, viewId);
   }

   @Override
   public void applyState(Collection<InternalCacheEntry> state,
                          Address sender, int viewId) throws InterruptedException {
      waitForStateTransferToStart(viewId);
      if (viewId < newViewId) {
         log.debugf("Rejecting state pushed by node %s for old rehash %d (last view id we know is %d)", sender, viewId, newViewId);
         return;
      }

      log.debugf("Applying new state from %s: received %d keys", sender, state.size());
      if (trace) log.tracef("Received keys: %s", keys(state));
      for (InternalCacheEntry e : state) {
         InvocationContext ctx = icc.createInvocationContext();
         // locking not necessary as during rehashing we block all transactions
         ctx.setFlags(CACHE_MODE_LOCAL, SKIP_CACHE_LOAD, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING,
                      SKIP_OWNERSHIP_CHECK);
         try {
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), ctx.getFlags());
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
   public boolean startStateTransfer(int viewId, Collection<Address> members, boolean initialView) throws TimeoutException, InterruptedException, PendingStateTransferException {
      // if this is not our first view, we'll send a push completed request afterwards
      if (initialView) {
         signalJoinStarted(viewId);
      }

      boolean clusterJoinConfirmed = pushConfirmations.waitForClusterToConfirmJoin(viewId, configuration.getRehashWaitTime());
      if (!clusterJoinConfirmed) {
         return false;
      }

      stateTransferInProgressLatch.close();
      return true;
   }

   public void endStateTransfer() {
      // we can now use the new CH as the baseline for the next rehash
      oldViewId = newViewId;
      chOld = chNew;

      stateTransferInProgressLatch.open();
      joinCompletedLatch.countDown();
   }

   void signalJoinStarted(int viewId) throws InterruptedException, TimeoutException, PendingStateTransferException {
      Address self = getAddress();

      if (trace) log.tracef("Node %s joining the cluster, broadcasting join request.", self, viewId);

      // the broadcast won't include the local node, call the method directly
      pushConfirmations.confirmJoin(self, viewId);
      // then broadcast to the entire cluster
      final StateTransferControlCommand cmd = cf.buildStateTransferCommand(StateTransferControlCommand.Type.REQUEST_JOIN, self, viewId);
      rpcManager.invokeRemotely(null, cmd, ResponseMode.SYNCHRONOUS, configuration.getRehashRpcTimeout());
   }

   public void signalPushCompleted(int viewId) throws InterruptedException, TimeoutException, PendingStateTransferException {
      Address self = getAddress();

      if (trace) log.tracef("Node %s finished pushing state for view %s, broadcasting push complete signal.", self, viewId);

      // the broadcast won't include the local node, call the method directly
      pushConfirmations.confirmPush(self, viewId);
      // then broadcast to the entire cluster
      final StateTransferControlCommand cmd = cf.buildStateTransferCommand(StateTransferControlCommand.Type.PUSH_COMPLETED, self, viewId);
      rpcManager.invokeRemotely(null, cmd, ResponseMode.SYNCHRONOUS, configuration.getRehashRpcTimeout());

      boolean clusterPushCompleted = pushConfirmations.waitForClusterToCompletePush(viewId, configuration.getRehashWaitTime());
      if (!clusterPushCompleted) {
         throw new PendingStateTransferException();
      }
   }

   public abstract CacheStore getCacheStoreForStateTransfer();

   public void pushStateToNode(NotifyingNotifiableFuture<Object> stateTransferFuture, int viewId, Address target, Collection<InternalCacheEntry> state) throws PendingStateTransferException {
      checkForPendingRehash(viewId);

      log.debugf("Pushing to node %s %d keys", target, state.size());
      log.tracef("Pushing to node %s keys: %s", target, keys(state));

      final StateTransferControlCommand cmd = cf.buildStateTransferCommand(StateTransferControlCommand.Type.APPLY_STATE, getAddress(), viewId, state);

      rpcManager.invokeRemotelyInFuture(Collections.singleton(target), cmd,
                                        false, stateTransferFuture, configuration.getRehashRpcTimeout());
   }

   public boolean isLastViewId(int viewId) {
      return viewId == newViewId;
   }

   protected void checkForPendingRehash(int viewId) throws PendingStateTransferException {
      if (viewId != newViewId) {
         throw new PendingStateTransferException();
      }
   }

   private void newViewReceived(int viewId, List<Address> members, boolean initialView, boolean mergeView) {
      log.tracef("Received new cluster view: %d %s", viewId, members);
      if (initialView) {
         pushConfirmations.initialViewReceived(viewId, members);
      } else {
         pushConfirmations.newViewReceived(viewId, members, mergeView);
      }

      newViewId = viewId;
      chNew = createConsistentHash(members);

      BaseStateTransferTask task = createStateTransferTask(viewId, members, initialView);
      rehashExecutor.submit(task);
   }

   protected abstract BaseStateTransferTask createStateTransferTask(int viewId, List<Address> members, boolean initialView);

   @Listener
   public class ViewChangeListener {
      @Merged
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         newViewReceived(e.getViewId(), e.getNewMembers(), false, e.isMergeView());
      }
   }
}
