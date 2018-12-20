package org.infinispan.statetransfer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private volatile int topologyId = -1;
   // future to topology equal to topologyId + 1
   private CompletableFuture<Void> topologyFuture = new CompletableFuture<>();

   private volatile int transactionDataTopologyId = -1;
   private CompletableFuture<Void> transactionDataFuture = new CompletableFuture<>();

   @Stop
   public void stop() {
      notifyTopologyInstalled(TOPOLOGY_ID_STOPPED);
      notifyTransactionDataReceived(TOPOLOGY_ID_STOPPED);
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
         log.debugf("Trying to set a topology id (%d) that is lower than the current one (%d)", topologyId,
                    this.topologyId);
         return;
      }
      if (trace) {
         log.tracef("Signalling transaction data received for topology %d", topologyId);
      }
      transactionDataTopologyId = topologyId;

      CompletableFuture<Void> oldFuture = null;
      try {
         synchronized (this) {
            oldFuture = transactionDataFuture;
            transactionDataFuture = new CompletableFuture<>();
         }
      } finally {
         if (oldFuture != null) {
            oldFuture.complete(null);
         }
      }
   }

   @Override
   public CompletableFuture<Void> transactionDataFuture(int expectedTopologyId) {
      if (transactionDataTopologyId >= expectedTopologyId)
         return CompletableFutures.completedNull();

      if (trace) {
         log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
                    transactionDataTopologyId);
      }
      synchronized (this){
         if (transactionDataTopologyId >= expectedTopologyId) {
            return CompletableFutures.completedNull();
         } else {
            return transactionDataFuture.thenCompose(nil -> transactionDataFuture(expectedTopologyId));
         }
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
         log.debugf("Trying to set a topology id (%d) that is lower than the current one (%d)", topologyId,
                    this.topologyId);
         return;
      }
      if (trace) {
         log.tracef("Signalling topology %d is installed", topologyId);
      }
      this.topologyId = topologyId;
      CompletableFuture<Void> oldFuture = null;
      try {
         synchronized (this) {
            oldFuture = topologyFuture;
            topologyFuture = new CompletableFuture<>();
         }
      } finally {
         if (oldFuture != null) {
            oldFuture.complete(null);
         }
      }
   }

   @Override
   public CompletableFuture<Void> topologyFuture(int expectedTopologyId) {
      if (topologyId >= expectedTopologyId)
         return CompletableFutures.completedNull();

      if (trace) {
         log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId, topologyId);
      }
      synchronized (this) {
         if (topologyId >= expectedTopologyId) {
            return CompletableFutures.completedNull();
         } else {
            return topologyFuture.thenCompose(nil -> topologyFuture(expectedTopologyId));
         }
      }
   }

   @Override
   public boolean topologyReceived(int expectedTopologyId) {
      return topologyId >= expectedTopologyId;
   }
}
