/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util.concurrent.locks;

import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Lock manager in charge with processing deadlock detections.
 * Implementation notes: if a deadlock is detected, then one of the transactions has to rollback. The transaction that
 * rollbacks is determined by comparing the coin toss from {@link org.infinispan.transaction.xa.DldGlobalTransaction}.
 * A thread calling {@link LockManager#lockAndRecord(Object, org.infinispan.context.InvocationContext, long)}
 * would run the deadlock detection algorithm only if all of the following take place:
 * - the call is made in the scope of a transaction (either locally originated or remotely originated)
 * - it cannot acquire lock on the given key and the lock owner is another transaction
 * - when comparing coin toss, this thread would loose against the other one - so it's always the potential loser that runs DLD.
 * If deadlock is detected then {@link LockManager#lockAndRecord(Object, org.infinispan.context.InvocationContext, long)} would throw an
 * {@link org.infinispan.util.concurrent.locks.DeadlockDetectedException}. This is subsequently handled in
 * in the interceptor chain - locks owned by this tx are released.
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

   private TimeService timeService;

   @Start
   public void init() {
      spinDuration = configuration.deadlockDetection().spinDuration();
      exposeJmxStats = configuration.jmxStatistics().enabled();
   }

   @Inject
   public void injectTimeService(TimeService timeService) {
      this.timeService = timeService;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long lockTimeout) throws InterruptedException {
      if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);

      if (ctx.isInTxScope()) {
         final long timeoutNanoTime = timeService.expectedEndTime(lockTimeout, MILLISECONDS);
         DldGlobalTransaction thisTx = (DldGlobalTransaction) ctx.getLockOwner();
         thisTx.setLockIntention(key);
         if (trace) log.tracef("Setting lock intention to %s for %s (%s)", key, thisTx, System.identityHashCode(thisTx));

         while (!timeService.isTimeExpired(timeoutNanoTime)) {
            if (lockContainer.acquireLock(ctx.getLockOwner(), key, spinDuration, MILLISECONDS) != null) {
               thisTx.setLockIntention(null); //clear lock intention
               if (trace) log.tracef("Successfully acquired lock on %s on behalf of %s.", key, ctx.getLockOwner());
               return true;
            } else {
               Object owner = getOwner(key);
               if (!(owner instanceof DldGlobalTransaction)) {
                  if (trace) log.tracef("Not running deadlock detection as lock owner (%s) is not transactional", owner);
                  cannotRunDld.incrementAndGet();
                  continue;
               }
               DldGlobalTransaction lockOwnerTx = (DldGlobalTransaction) owner;
               if (trace) log.tracef("Could not acquire lock as %s is locked by %s (%s)", key, owner, System.identityHashCode(owner));
               if (isDeadlockAndIAmLoosing(lockOwnerTx, thisTx, key)) {
                  updateStats(thisTx);
                  String message = String.format("Deadlock found and we (%s) shall not continue. Other tx is %s",
                                                 thisTx, lockOwnerTx);
                  log.trace(message);
                  throw new DeadlockDetectedException(message);
               }
            }
         }
      } else {
         return super.lockAndRecord(key, ctx, lockTimeout);
      }
      // couldn't acquire lock!
      return false;
   }

   private boolean isDeadlockAndIAmLoosing(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
      //run the lose check first as it is cheaper
      boolean wouldWeLoose = thisTx.wouldLose(lockOwnerTx);
      if (!wouldWeLoose) {
         if (trace) log.tracef("We (%s) win against the other (%s) transaction, so no point running rest of DLD", thisTx, lockOwnerTx);
         return false;
      }
      //do we have lock on what other tx intends to acquire?
      return ownsLocalIntention(thisTx, lockOwnerTx.getLockIntention()) || ownsRemoteIntention(lockOwnerTx, thisTx, key) || isSameKeyDeadlock(key, thisTx, lockOwnerTx);
   }

   private boolean isSameKeyDeadlock(Object key, DldGlobalTransaction thisTx, DldGlobalTransaction lockOwnerTx) {
      boolean iHaveRemoteLock = !thisTx.isRemote(); //this relies on the fact that when DLD is enabled a lock is first acquired remotely and then locally
      boolean otherHasLocalLock = lockOwnerTx.isRemote();

      //if we are here then 1) the other tx has a lock on this local key AND 2) I have a lock on the same key remotely
      if (iHaveRemoteLock && otherHasLocalLock) {
         if (trace) log.tracef("Same key deadlock between %s and %s on key %s.", thisTx, lockOwnerTx, key);
         return true;
      }
      return false;
   }

   /**
    * This happens with two nodes replicating same tx at the same time.
    */
   private boolean ownsRemoteIntention(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
      boolean localLockOwner = !lockOwnerTx.isRemote();
      if (localLockOwner) {
         // I've already acquired lock on this key before replicating here, so this mean we are in deadlock. This assumes the fact that
         // if trying to acquire a remote lock, a tx first acquires a local lock.
         if (thisTx.hasLockAtOrigin(lockOwnerTx.getRemoteLockIntention())) {
            if (trace)
               log.tracef("Same key deadlock detected: lock owner tries to acquire lock remotely on %s but we have it!", key);
            return true;
         }
      } else {
         if (trace) log.tracef("Lock owner is remote: %s", lockOwnerTx);
      }
      return false;
   }

   private boolean ownsLocalIntention(DldGlobalTransaction thisTx, Object lockOwnerTxIntention) {
      boolean result = lockOwnerTxIntention != null && ownsLock(lockOwnerTxIntention, thisTx);
      if (trace) log.tracef("Local intention is '%s'. Do we own lock for it? %s, We == %s", lockOwnerTxIntention, result, thisTx);
      return result;
   }

   public void setExposeJmxStats(boolean exposeJmxStats) {
      this.exposeJmxStats = exposeJmxStats;
   }


   @ManagedAttribute (description = "Total number of local detected deadlocks", displayName = "Number of total detected deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getTotalNumberOfDetectedDeadlocks() {
      return localTxStopped.get() + remoteTxStopped.get();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      localTxStopped.set(0);
      remoteTxStopped.set(0);
      cannotRunDld.set(0);
   }

   @ManagedAttribute(description = "Number of remote transaction that were roll backed due to deadlocks", displayName = "Number of remote transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedRemoteDeadlocks() {
      return remoteTxStopped.get();
   }

   @ManagedAttribute (description = "Number of local transaction that were roll backed due to deadlocks", displayName = "Number of local transaction that were roll backed due to deadlocks", measurementType = MeasurementType.TRENDSUP)
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


   @ManagedAttribute(description = "Number of locally originated transactions that were interrupted as a deadlock situation was detected", displayName = "Number of interrupted local transactions", measurementType = MeasurementType.TRENDSUP)
   @Deprecated
   public static long getLocallyInterruptedTransactions() {
      return -1;
   }

}
