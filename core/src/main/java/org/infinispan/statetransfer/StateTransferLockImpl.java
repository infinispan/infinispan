package org.infinispan.statetransfer;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.locks.ReadWriteLock;
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
   private final Object topologyLock = new Object();

   private volatile int transactionDataTopologyId;
   private final Object transactionDataLock = new Object();

   @Override
   public void acquireExclusiveTopologyLock() {
      ownershipLock.writeLock().lock();
   }

   @Override
   public void releaseExclusiveTopologyLock() {
      ownershipLock.writeLock().unlock();
   }

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
      synchronized (transactionDataLock) {
         transactionDataLock.notifyAll();
      }
   }

   @Override
   public void waitForTransactionData(int expectedTopologyId) throws InterruptedException {
      if (trace) {
         log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
               transactionDataTopologyId);
      }

      if (transactionDataTopologyId >= expectedTopologyId)
         return;

      synchronized (transactionDataLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (transactionDataTopologyId < expectedTopologyId) {
            transactionDataLock.wait();
         }
      }
      if (trace) {
         log.tracef("Received transaction data for topology %d, expected topology was %d", transactionDataTopologyId,
               expectedTopologyId);
      }
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
      synchronized (topologyLock) {
         topologyLock.notifyAll();
      }
   }

   @Override
   public void waitForTopology(int expectedTopologyId) throws InterruptedException {
      if (topologyId >= expectedTopologyId)
         return;

      if (trace) {
         log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId, topologyId);
      }
      synchronized (topologyLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (topologyId < expectedTopologyId) {
            topologyLock.wait();
         }
      }
      if (trace) {
         log.tracef("Topology %d is now installed, expected topology was %d", topologyId, expectedTopologyId);
      }
   }
}
