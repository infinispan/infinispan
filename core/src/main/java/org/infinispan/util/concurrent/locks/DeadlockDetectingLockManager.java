package org.infinispan.util.concurrent.locks;

import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock manager in charge with processing deadlock detections.
 * Implementation notes: if a deadlock is detected, then one of the transactions has to rollback. The transaction that
 * rollbacks is determined by comparing the coin toss from {@link org.infinispan.transaction.xa.DldGlobalTransaction}.
 * A thread calling {@link DeadlockDetectingLockManager#lockAndRecord(Object, org.infinispan.context.InvocationContext)}
 * would run the deadlock detection algorithm only if all of the following take place:
 * - the call is made in the scope of a transaction (either locally originated or remotely originated)
 * - it cannot acquire lock on the given key and the lock owner is another transaction
 * - when comparing coin toss, this thread would loose against the other one - so it's always the potential loser that runs DLD.
 * If deadlock is detected then {@link #lockAndRecord(Object, org.infinispan.context.InvocationContext)} would throw an
 * {@link org.infinispan.util.concurrent.locks.DeadlockDetectedException}. This is subsequently handled in
 * in the interceptor chain - locsk owned by this tx are released.
 *
 * @author Mircea.Markus@jboss.com
 */
@MBean(objectName = "DeadlockDetectingLockManager", description = "Information about the number of deadlocks that were detected")
public class DeadlockDetectingLockManager extends LockManagerImpl {

   private static final Log log = LogFactory.getLog(DeadlockDetectingLockManager.class);

   protected volatile long spinDuration;

   protected volatile boolean exposeJmxStats;

   private AtomicLong localTxStopped = new AtomicLong(0);

   private AtomicLong remoteTxStopped = new AtomicLong(0);

   private AtomicLong cannotRunDld = new AtomicLong(0);

   @Start
   public void init() {
      spinDuration = configuration.getDeadlockDetectionSpinDuration();
      exposeJmxStats = configuration.isExposeJmxStatistics();
   }

   public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
      long lockTimeout = getLockAcquisitionTimeout(ctx);
      if (trace) log.trace("Attempting to lock {0} with acquisition timeout of {1} millis", key, lockTimeout);


      if (ctx.isInTxScope()) {
         if (trace) log.trace("Using early dead lock detection");
         final long start = System.currentTimeMillis();
         DldGlobalTransaction thisTx = (DldGlobalTransaction) ctx.getLockOwner();
         thisTx.setLockLocalLockIntention(key);
         if (trace) log.trace("Setting lock intention to: " + key);

         while (System.currentTimeMillis() < (start + lockTimeout)) {
            if (lockContainer.acquireLock(key, spinDuration, MILLISECONDS) != null) {
               thisTx.setLockLocalLockIntention(null); //clear lock intention
               if (trace) log.trace("successfully acquired lock on " + key + ", returning ...");
               return true;
            } else {
               Object owner = getOwner(key);
               if (!(owner instanceof DldGlobalTransaction)) {
                  if (trace) log.trace("Not running DLD as lock owner( " + owner + ") is not a transaction");
                  cannotRunDld.incrementAndGet();
                  continue;
               }
               DldGlobalTransaction lockOwnerTx = (DldGlobalTransaction) owner;
               if (isDeadlockAndIAmLoosing(lockOwnerTx, thisTx, key)) {
                  updateStats(thisTx);
                  String message = "Deadlock found and we " + thisTx + " shall not continue. Other tx is " + lockOwnerTx;
                  if (trace) log.trace(message);
                  throw new DeadlockDetectedException(message);
               }
            }
         }
      } else {
         if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS) != null) {
            return true;
         }
      }
      // couldn't acquire lock!
      return false;
   }

   private boolean isDeadlockAndIAmLoosing(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
      //run the lose check first as it is cheaper
      boolean wouldWeLoose = thisTx.wouldLose(lockOwnerTx);
      if (!wouldWeLoose) {
         if (trace) log.trace("We (" + thisTx + ") wouldn't lose against the other(" + lockOwnerTx + ") transaction, so no point running rest of DLD");
         return false;
      }
      //do we have lock on what other tx intends to acquire?
      return ownsLocalIntention(thisTx, lockOwnerTx.getLockIntention()) || ownsRemoteIntention(lockOwnerTx, thisTx, key);
   }

   /**
    * This happens with two nodes replicating same tx at the same time.
    */
   private boolean ownsRemoteIntention(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
      if (!lockOwnerTx.isRemote()) {
         // I've already acquired lock on this key before replicating here, so this mean we are in deadlock. This assumes the fact that
         // if trying to acquire a remote lock, a tx first acquires a local lock. This stands true in all situations but
         // when DLD + eager locking is used (in this scenario remote locks are acquired first).
         if (lockOwnerTx.isAcquiringRemoteLock(key, thisTx.getAddress())) {
            if (trace)
               log.trace("Same key deadlock detected: lock owner tries to acquire lock remotely on " + key + " but we have it!");
            return true;
         }
         for (Object remoteIntention : lockOwnerTx.getRemoteLockIntention()) {
            if (ownsLock(remoteIntention, thisTx)) {
                  if (trace) log.trace("We own lock on a key ('" + remoteIntention + "') on which other tx wants to acquire remote lock");
               return true;
            }
         }
      }
      return false;
   }

   private boolean ownsLocalIntention(DldGlobalTransaction tx, Object intention) {
      boolean result = intention != null && ownsLock(intention, tx);
      if (trace) log.trace("Intention is '" + intention + "'. Do we own lock for it? " + result + " We == " + tx);
      return result;
   }

   public void setExposeJmxStats(boolean exposeJmxStats) {
      this.exposeJmxStats = exposeJmxStats;
   }


   @ManagedAttribute (description = "Total number of local detected deadlocks")
   @Metric(displayName = "Number of total detected deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getTotalNumberOfDetectedDeadlocks() {
      return localTxStopped.get() + remoteTxStopped.get();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      localTxStopped.set(0);
      remoteTxStopped.set(0);
      cannotRunDld.set(0); 
   }

   @ManagedAttribute(description = "Number of remote transaction that were roll backed due to deadlocks")
   @Metric(displayName = "Number of remote transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedRemoteDeadlocks() {
      return remoteTxStopped.get();
   }

   @ManagedAttribute (description = "Number of local transaction that were roll backed due to deadlocks")
   @Metric(displayName = "Number of local transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedLocalDeadlocks() {
      return localTxStopped.get();
   }

   @ManagedAttribute(description = "Number of situtations when we try to determine a deadlock and the other lock owner is NOT a transaction. In this scenario we cannot run the deadlock detection mechanism")
   @Metric(displayName = "Number of unsolvable deadlock situations", measurementType = MeasurementType.TRENDSUP)
   public long getOverlapWithNotDeadlockAwareLockOwners() {
      return cannotRunDld.get();
   }



   private void updateStats(DldGlobalTransaction tx) {
      if (exposeJmxStats) {
         if (tx.isRemote())
            remoteTxStopped.incrementAndGet();
         else
            localTxStopped.incrementAndGet();
      }
   }


   @ManagedAttribute(description = "Number of locally originated transactions that were interrupted as a deadlock situation was detected")
   @Metric(displayName = "Number of interrupted local transactions", measurementType = MeasurementType.TRENDSUP)
   @Deprecated
   public long getLocallyInterruptedTransactions() {
      return -1;
   }

}
