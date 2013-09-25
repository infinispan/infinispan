package org.infinispan.statetransfer;

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

   private final ReadWriteLock ownershipLock = new ReentrantReadWriteLock();

   private volatile int topologyId;
   private final Lock topologyLock = new ReentrantLock();
   private final Condition topologyCondition = topologyLock.newCondition();

   private volatile int transactionDataTopologyId;
   private final Lock transactionDataLock = new ReentrantLock();
   private final Condition transactionDataCondition = transactionDataLock.newCondition();

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
      if (trace) {
         log.tracef("Signalling topology %d is installed", topologyId);
      }
      this.topologyId = topologyId;

      topologyLock.lock();
      try {
         topologyCondition.signalAll();
      } finally {
         topologyLock.unlock();
      }
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
         while (topologyId < expectedTopologyId && timeoutNanos > 0) {
            timeoutNanos = topologyCondition.awaitNanos(timeoutNanos);
         }
         if (timeoutNanos <= 0) {
            throw new TimeoutException("Timed out waiting for topology " + expectedTopologyId);
         }
      } finally {
         topologyLock.unlock();
      }
      if (trace) {
         log.tracef("Topology %d is now installed, expected topology was %d", topologyId, expectedTopologyId);
      }
   }
}
