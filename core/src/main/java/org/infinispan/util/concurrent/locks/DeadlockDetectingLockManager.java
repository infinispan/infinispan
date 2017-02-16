package org.infinispan.util.concurrent.locks;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Lock manager in charge with processing deadlock detections.
 * </p>
 * Implementation notes: if a deadlock is detected, then one of the transactions has to rollback. The transaction that
 * rollbacks is determined by comparing the coin toss from {@link org.infinispan.transaction.xa.DldGlobalTransaction}.
 * </p>
 * A thread calling {@link #lock(Object, Object, long, TimeUnit)} or {@link #lockAll(Collection, Object, long, TimeUnit)}
 * would run the deadlock detection algorithm only if all of the following take place:
 * - the call is made in the scope of a transaction (either locally originated or remotely originated)
 * - it cannot acquire lock on the given key and the lock owner is another transaction
 * - when comparing coin toss, this thread would loose against the other one - so it's always the potential loser that runs DLD.
 * If deadlock is detected then a {@link org.infinispan.util.concurrent.locks.DeadlockDetectedException} is thrown.
 * This is subsequently handled in the interceptor chain - locks owned by this tx are released.
 *
 * @author Mircea.Markus@jboss.com
 */
@MBean(objectName = "DeadlockDetectingLockManager", description = "Information about the number of deadlocks that were detected")
public class DeadlockDetectingLockManager extends DefaultLockManager implements DeadlockChecker, Runnable {

   private static final Log log = LogFactory.getLog(DeadlockDetectingLockManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private ScheduledFuture<?> scheduledFuture;

   protected volatile boolean exposeJmxStats;

   private AtomicLong localTxStopped = new AtomicLong(0);

   private AtomicLong remoteTxStopped = new AtomicLong(0);

   private AtomicLong cannotRunDld = new AtomicLong(0);

   @Start
   public void init() {
      long spinDuration = configuration.deadlockDetection().spinDuration();
      exposeJmxStats = configuration.jmxStatistics().enabled();
      scheduledFuture = scheduler.scheduleWithFixedDelay(this, spinDuration, spinDuration, TimeUnit.MILLISECONDS);
   }

   @Stop
   public void stopScheduler() {
      if (scheduledFuture != null) {
         scheduledFuture.cancel(false);
         scheduledFuture = null;
      }
   }

   @Override
   public KeyAwareLockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit) {
      if (lockOwner instanceof DldGlobalTransaction) {
         ((DldGlobalTransaction) lockOwner).setLockIntention(Collections.singleton(key));
         return super.lock(key, lockOwner, time, unit);
      }
      return super.lock(key, lockOwner, time, unit);
   }

   @Override
   public KeyAwareLockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit) {
      if (lockOwner instanceof DldGlobalTransaction) {
         ((DldGlobalTransaction) lockOwner).setLockIntention(new HashSet<>(keys));
         return super.lockAll(keys, lockOwner, time, unit);
      }
      return super.lockAll(keys, lockOwner, time, unit);
   }

   @Override
   public void run() {
      lockContainer.deadlockCheck(this);
   }

   private boolean isDeadlockAndIAmLoosing(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx) {
      //run the lose check first as it is cheaper
      boolean wouldWeLoose = thisTx.wouldLose(lockOwnerTx);
      if (!wouldWeLoose) {
         if (trace)
            log.tracef("We (%s) win against the other (%s) transaction, so no point running rest of DLD", thisTx, lockOwnerTx);
         return false;
      }
      //do we have lock on what other tx intends to acquire?
      return ownsAnyLocalIntention(thisTx, lockOwnerTx) || ownsRemoteIntention(lockOwnerTx, thisTx) || isSameKeyDeadlock(thisTx, lockOwnerTx);
   }

   private boolean isSameKeyDeadlock(DldGlobalTransaction thisTx, DldGlobalTransaction lockOwnerTx) {
      boolean iHaveRemoteLock = !thisTx.isRemote(); //this relies on the fact that when DLD is enabled a lock is first acquired remotely and then locally
      boolean otherHasLocalLock = lockOwnerTx.isRemote();

      //if we are here then 1) the other tx has a lock on this local key AND 2) I have a lock on the same key remotely
      if (iHaveRemoteLock && otherHasLocalLock) {
         if (trace) log.tracef("Same key deadlock between %s and %s.", thisTx, lockOwnerTx);
         return true;
      }
      return false;
   }

   /**
    * This happens with two nodes replicating same tx at the same time.
    */
   private boolean ownsRemoteIntention(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx) {
      boolean localLockOwner = !lockOwnerTx.isRemote();
      if (localLockOwner) {
         // I've already acquired lock on this key before replicating here, so this mean we are in deadlock. This assumes the fact that
         // if trying to acquire a remote lock, a tx first acquires a local lock.
         if (thisTx.hasAnyLockAtOrigin(lockOwnerTx)) {
            if (trace)
               log.trace("Same key deadlock detected: lock owner tries to acquire a lock remotely but we have it!");
            return true;
         }
      } else {
         if (trace) log.tracef("Lock owner is remote: %s", lockOwnerTx);
      }
      return false;
   }

   private boolean ownsAnyLocalIntention(DldGlobalTransaction thisTx, DldGlobalTransaction otherTx) {
      for (Object key : otherTx.getLockIntention()) {
         if (ownsLock(key, thisTx)) {
            if (trace) log.tracef("Local intention is '%s' and we (%s) own the lock.", key, thisTx);
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean deadlockDetected(Object pendingOwner, Object currentOwner) {
      if (!(pendingOwner instanceof DldGlobalTransaction) || !(currentOwner instanceof DldGlobalTransaction)) {
         if (trace) {
            log.tracef("Unable to run DLD with %s and %s. One of them are not a DldGlobalTransaction.",
                       pendingOwner, currentOwner);
         }
         cannotRunDld.incrementAndGet();
         return false;
      }
      if (trace) {
         log.tracef("Could not acquire lock. It is locked by %s (%s)", currentOwner, System.identityHashCode(currentOwner));
      }

      final DldGlobalTransaction ownerTx = (DldGlobalTransaction) currentOwner;
      final DldGlobalTransaction pendingTx = (DldGlobalTransaction) pendingOwner;

      if (isDeadlockAndIAmLoosing(ownerTx, pendingTx)) {
         updateStats(pendingTx);
         log.tracef("Deadlock found and we (%s) shall not continue. Other tx is %s", pendingOwner, currentOwner);
         return true;
      }
      return false;
   }

   public void setExposeJmxStats(boolean exposeJmxStats) {
      this.exposeJmxStats = exposeJmxStats;
   }

   @ManagedAttribute(description = "Total number of local detected deadlocks", displayName = "Number of total detected deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getTotalNumberOfDetectedDeadlocks() {
      return localTxStopped.get() + remoteTxStopped.get();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      localTxStopped.set(0);
      remoteTxStopped.set(0);
      cannotRunDld.set(0);
   }

   @ManagedAttribute(description = "Number of remote transactions that were rolled-back due to deadlocks", displayName = "Number of remote transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedRemoteDeadlocks() {
      return remoteTxStopped.get();
   }

   @ManagedAttribute(description = "Number of local transactions that were rolled-back due to deadlocks", displayName = "Number of local transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedLocalDeadlocks() {
      return localTxStopped.get();
   }

   @ManagedAttribute(description = "Number of situations when we try to determine a deadlock and the other lock owner is NOT a transaction. In this scenario we cannot run the deadlock detection mechanism", displayName = "Number of unsolvable deadlock situations", measurementType = MeasurementType.TRENDSUP)
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
}
