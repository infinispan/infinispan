package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
   private static final int NO_TOPOLOGY = -1;
   private static final int STOPPED = -2;

   private final ReadWriteLock ownershipLock = new ReentrantReadWriteLock();

   private volatile int topologyId = NO_TOPOLOGY;
   private final Lock topologyLock = new ReentrantLock();
   private final Condition topologyCondition = topologyLock.newCondition();

   private volatile int transactionDataTopologyId = NO_TOPOLOGY;
   private final Lock transactionDataLock = new ReentrantLock();
   private final Condition transactionDataCondition = transactionDataLock.newCondition();

   @Start
   public void start() {
      this.topologyId = NO_TOPOLOGY;
   }

   @Stop(priority = 21) //after StateTransferManager
   public void stop() {
      this.topologyId = STOPPED;
      notifyTopologyId();
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
      transactionDataLock.lock();
      try {
         transactionDataCondition.signalAll();
      } finally {
         transactionDataLock.unlock();
      }
   }

   @Override
   public void waitForTransactionData(int expectedTopologyId, long timeout,
                                      TimeUnit unit) throws InterruptedException {
      if (trace) {
         log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
               transactionDataTopologyId);
      }

      if (transactionDataTopologyId >= expectedTopologyId)
         return;

      transactionDataLock.lock();
      try {
         long timeoutNanos = unit.toNanos(timeout);
         while (transactionDataTopologyId < expectedTopologyId && timeoutNanos > 0) {
            timeoutNanos = transactionDataCondition.awaitNanos(timeoutNanos);
         }
         if (timeoutNanos <= 0) {
            throw new TimeoutException("Timed out waiting for topology " + expectedTopologyId);
         }
      } finally {
         transactionDataLock.unlock();
      }
      if (trace) {
         log.tracef("Received transaction data for topology %d, expected topology was %d", transactionDataTopologyId,
               expectedTopologyId);
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
      this.topologyId = topologyId;
      notifyTopologyId();
   }

   @Override
   public void waitForTopology(int expectedTopologyId, long timeout, TimeUnit unit) throws InterruptedException {
      if (topologyId >= expectedTopologyId)
         return;

      if (trace) {
         log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId, topologyId);
      }
      topologyLock.lock();
      try {
         long timeoutNanos = unit.toNanos(timeout);
         do {
            final int currentTopologyId = topologyId; //avoid multiple reads
            if (currentTopologyId >= expectedTopologyId) {
               break;
            } else if (currentTopologyId == STOPPED) {
               throw new CacheException("Cache is stopped.");
            } else if (timeoutNanos <= 0) {
               throw new TimeoutException("Timed out waiting for topology " + expectedTopologyId +
                                                ". Current topology is " + currentTopologyId);
            }
            timeoutNanos = topologyCondition.awaitNanos(timeoutNanos);
         } while (true);
      } finally {
         topologyLock.unlock();
      }
      if (trace) {
         log.tracef("Topology %d is now installed, expected topology was %d", topologyId, expectedTopologyId);
      }
   }

   @Override
   public boolean topologyReceived(int expectedTopologyId) {
      return topologyId >= expectedTopologyId;
   }

   private void notifyTopologyId() {
      if (trace) {
         log.tracef("Signalling topology %d is installed", topologyId);
      }
      topologyLock.lock();
      try {
         topologyCondition.signalAll();
      } finally {
         topologyLock.unlock();
      }
   }
}
