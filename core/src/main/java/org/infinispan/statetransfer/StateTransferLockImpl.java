package org.infinispan.statetransfer;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

/**
 * {@code StateTransferLock} implementation.
 *
 * @author anistor@redhat.com
 * @author Dan Berindei
 * @since 5.2
 */
public class StateTransferLockImpl implements StateTransferLock {
   private static final Log log = LogFactory.getLog(StateTransferLockImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int TOPOLOGY_ID_STOPPED = Integer.MAX_VALUE;

   private final ReadWriteLock ownershipLock = new ReentrantReadWriteLock();
   private Executor remoteExecutor;

   private volatile int topologyId = -1;
   private final Lock topologyLock = new ReentrantLock();
   private final Map<Integer, CompletableFuture<Void>> topologyFutures = new HashMap<>();

   private volatile int transactionDataTopologyId = -1;
   private final Lock transactionDataLock = new ReentrantLock();
   private final Map<Integer, CompletableFuture<Void>> transactionDataFutures = new HashMap<>();


   @Inject
   public void init(@ComponentName(REMOTE_COMMAND_EXECUTOR) Executor executor) {
      this.remoteExecutor = executor;
   }

   public void stop() {
      notifyTransactionDataReceived(TOPOLOGY_ID_STOPPED);
      notifyTopologyInstalled(TOPOLOGY_ID_STOPPED);
   }

   @SuppressWarnings("LockAcquiredButNotSafelyReleased")
   @Override
   public void acquireExclusiveTopologyLock() {
      ownershipLock.writeLock().lock();
   }

   @Override
   public void releaseExclusiveTopologyLock() {
      ownershipLock.writeLock().unlock();
   }

   @SuppressWarnings("LockAcquiredButNotSafelyReleased")
   @Override
   public void acquireSharedTopologyLock() {
      ownershipLock.readLock().lock();
   }

   @Override
   public void releaseSharedTopologyLock() {
      ownershipLock.readLock().unlock();
   }

   @Override
   public void notifyTransactionDataReceived(int topologyId) {
      if (topologyId < transactionDataTopologyId) {
         throw new IllegalStateException("Cannot set a topology id (" + topologyId +
               ") that is lower than the current one (" + transactionDataTopologyId + ")");
      }
      if (trace) {
         log.tracef("Signalling transaction data received for topology %d", topologyId);
      }
      transactionDataTopologyId = topologyId;
      completeFutures(topologyId, transactionDataLock, transactionDataFutures);
   }

   @Override
   public CompletableFuture<Void> transactionDataFuture(int expectedTopologyId) throws InterruptedException {
      if (transactionDataTopologyId >= expectedTopologyId)
         return null;

      if (trace) {
         log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
                    transactionDataTopologyId);
      }
      transactionDataLock.lock();
      try {
         if (transactionDataTopologyId < expectedTopologyId) {
            return transactionDataFutures.computeIfAbsent(expectedTopologyId, k -> new CompletableFuture<>());
         } else {
            return null;
         }
      } finally {
         transactionDataLock.unlock();
      }
   }

   @Override
   public boolean transactionDataReceived(int expectedTopologyId) {
      if (trace) log.tracef("Checking if transaction data was received for topology %s, current topology is %s",
            expectedTopologyId, transactionDataTopologyId);
      return transactionDataTopologyId >= expectedTopologyId;
   }

   @Override
   public void notifyTopologyInstalled(int topologyId) {
      if (topologyId < this.topologyId) {
         throw new IllegalStateException("Cannot set a topology id (" + topologyId +
               ") that is lower than the current one (" + this.topologyId + ")");
      }
      if (trace) {
         log.tracef("Signalling topology %d is installed", topologyId);
      }
      this.topologyId = topologyId;
      completeFutures(topologyId, topologyLock, topologyFutures);
   }

   private void completeFutures(int topologyId, Lock lock, Map<Integer, CompletableFuture<Void>> futures) {
      List<CompletableFuture<Void>> toComplete = new ArrayList<>();
      lock.lock();
      try {
         for (Iterator<Map.Entry<Integer, CompletableFuture<Void>>> it = futures.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, CompletableFuture<Void>> entry = it.next();
            if (entry.getKey() <= topologyId) {
               // remoteExecutor can have caller-runs policy, so don't complete the future when holding the lock
               toComplete.add(entry.getValue());
               it.remove();
            }
         }
      } finally {
         lock.unlock();
         for (CompletableFuture<Void> future : toComplete) {
            remoteExecutor.execute(() -> future.complete(null));
         }
      }
   }

   @Override
   public CompletableFuture<Void> topologyFuture(int expectedTopologyId) throws InterruptedException {
      if (topologyId >= expectedTopologyId)
         return null;

      if (trace) {
         log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId, topologyId);
      }
      topologyLock.lock();
      try {
         if (topologyId < expectedTopologyId) {
            return topologyFutures.computeIfAbsent(expectedTopologyId, k -> new CompletableFuture<>());
         } else {
            return null;
         }
      } finally {
         topologyLock.unlock();
      }
   }

   private void reportErrorAfterWait(int expectedTopologyId, long timeoutNanos) {
      if (timeoutNanos <= 0) {
         throw new TimeoutException("Timed out waiting for topology " + expectedTopologyId);
      }
      if (topologyId == TOPOLOGY_ID_STOPPED) {
         throw new IllegalLifecycleStateException("Cache was stopped while waiting for topology " + expectedTopologyId);
      }
   }

   @Override
   public boolean topologyReceived(int expectedTopologyId) {
      return topologyId >= expectedTopologyId;
   }
}
