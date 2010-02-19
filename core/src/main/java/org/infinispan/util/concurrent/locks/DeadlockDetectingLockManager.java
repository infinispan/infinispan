package org.infinispan.util.concurrent.locks;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.DeadlockDetectingGlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock manager in charge with processing deadlock detections.
 *
 * @author Mircea.Markus@jboss.com
 */
@MBean(objectName = "DeadlockDetectingLockManager", description = "Information about the number of deadlocks that were detected")
public class DeadlockDetectingLockManager extends LockManagerImpl {

   private static final Log log = LogFactory.getLog(DeadlockDetectingLockManager.class);

   protected volatile long spinDuration;

   protected volatile boolean exposeJmxStats;

   private volatile boolean isSync;

   private AtomicLong detectedRemoteDeadlocks = new AtomicLong(0);

   private AtomicLong detectedLocalDeadlocks = new AtomicLong(0);

   private AtomicLong locallyInterruptedTransactions = new AtomicLong(0);

   private AtomicLong overlapWithNotDeadlockAwareLockOwners = new AtomicLong(0);

   @Start
   public void init() {
      spinDuration = configuration.getDeadlockDetectionSpinDuration();
      exposeJmxStats = configuration.isExposeJmxStatistics();
      isSync = configuration.getCacheMode().isSynchronous();
   }

   public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
      long lockTimeout = getLockAcquisitionTimeout(ctx);
      if (trace) log.trace("Attempting to lock {0} with acquisition timeout of {1} millis", key, lockTimeout);

      if (ctx.isInTxScope()) {
         if (trace) log.trace("Using early dead lock detection");
         final long start = System.currentTimeMillis();
         long now;
         while ((now = System.currentTimeMillis()) < (start + lockTimeout)) {
            if (lockContainer.acquireLock(key, spinDuration, MILLISECONDS)) {
               if (trace) log.trace("successfully acquired lock on " + key + ", returning ...");
               return true;
            } else {
               if (trace)
                  log.trace("Could not acquire lock on '" + key + "' as it is locked by '" + getOwner(key) + "', check for dead locks");
               Object owner = getOwner(key);
               if (!(owner instanceof DeadlockDetectingGlobalTransaction)) {
                  if (trace)
                     log.trace("Owner is not instance of DeadlockDetectingGlobalTransaction: " + owner + ", continuing ...");
                  if (exposeJmxStats) overlapWithNotDeadlockAwareLockOwners.incrementAndGet();
                  continue; //try to acquire lock again, for the rest of the time
               }
               DeadlockDetectingGlobalTransaction lockOwnerTx = (DeadlockDetectingGlobalTransaction) owner;
               if (isSync && !ctx.isOriginLocal() && !lockOwnerTx.isRemote()) {
                  return remoteVsRemoteDld(key, ctx, lockTimeout, start, now, lockOwnerTx);
               }
               if ((ctx.isOriginLocal() && !lockOwnerTx.isRemote()) || (!isSync && !ctx.isOriginLocal() && !lockOwnerTx.isRemote())) {
                  localVsLocalDld(ctx, lockOwnerTx);
               }
            }
         }
      } else {
         if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS)) {
            return true;
         }
      }
      // couldn't acquire lock!
      return false;
   }

   private void localVsLocalDld(InvocationContext ctx, DeadlockDetectingGlobalTransaction lockOwnerTx) {
      if (trace) log.trace("Looking for local vs local deadlocks");
      DeadlockDetectingGlobalTransaction thisThreadsTx = (DeadlockDetectingGlobalTransaction) ctx.getLockOwner();
      boolean weOwnLock = ownsLock(lockOwnerTx.getLockIntention(), thisThreadsTx);
      if (trace) {
         log.trace("Other owner's intention is " + lockOwnerTx.getLockIntention() + ". Do we(" + thisThreadsTx + ") own lock for it? " + weOwnLock + ". Lock owner is " + getOwner(lockOwnerTx.getLockIntention()));
      }
      if (weOwnLock) {
         boolean iShouldInterrupt = thisThreadsTx.thisWillInterrupt(lockOwnerTx);
         if (trace)
            log.trace("deadlock situation detected. Shall I interrupt?" + iShouldInterrupt );
         if (iShouldInterrupt) {
            lockOwnerTx.interruptProcessingThread();
            if (exposeJmxStats) detectedLocalDeadlocks.incrementAndGet();
         }
      }
   }

   private boolean remoteVsRemoteDld(Object key, InvocationContext ctx, long lockTimeout, long start, long now, DeadlockDetectingGlobalTransaction lockOwnerTx) throws InterruptedException {
      TxInvocationContext remoteTxContext = (TxInvocationContext) ctx;
      Address origin = remoteTxContext.getGlobalTransaction().getAddress();
      DeadlockDetectingGlobalTransaction remoteGlobalTransaction = (DeadlockDetectingGlobalTransaction) ctx.getLockOwner();
      boolean thisShouldInterrupt = remoteGlobalTransaction.thisWillInterrupt(lockOwnerTx);
      if (trace) log.trace("Should I interrupt other transaction ? " + thisShouldInterrupt);
      boolean isDeadLock = (configuration.getCacheMode().isReplicated() || lockOwnerTx.isReplicatingTo(origin)) && !lockOwnerTx.isRemote();
      if (thisShouldInterrupt && isDeadLock) {
         lockOwnerTx.interruptProcessingThread();
         if (exposeJmxStats) {
            detectedRemoteDeadlocks.incrementAndGet();
            locallyInterruptedTransactions.incrementAndGet();
         }
         return lockForTheRemainingTime(key, lockTimeout, start, now);
      } else if (!isDeadLock) {
         return lockForTheRemainingTime(key, lockTimeout, start, now);
      } else {
         if (trace)
            log.trace("Not trying to acquire lock anymore, as we're in deadlock and this will be rollback at origin");
         if (exposeJmxStats) {
            detectedRemoteDeadlocks.incrementAndGet();
         }
         remoteGlobalTransaction.setMarkedForRollback(true);
         throw new DeadlockDetectedException("Deadlock situation detected on tx: " + remoteTxContext.getLockOwner());
      }
   }

   private boolean lockForTheRemainingTime(Object key, long lockTimeout, long start, long now) throws InterruptedException {
      long remainingLockingTime = (start + lockTimeout) - now;
      if (remainingLockingTime < 0)
         throw new IllegalStateException("No remaining time!!! The outer while condition MUST make sure this always stands true!");
      if (trace) log.trace("trying to lock for the remaining time: " + remainingLockingTime + " millis ");
      return lockContainer.acquireLock(key, remainingLockingTime, MILLISECONDS);
   }

   public void setExposeJmxStats(boolean exposeJmxStats) {
      this.exposeJmxStats = exposeJmxStats;
   }

   @ManagedAttribute(description = "Number of situtations when we try to determine a deadlock and the other lock owner is e.g. a local tx. In this scenario we cannot run the deadlock detection mechanism")
   @Metric(displayName = "Number of unsolvable deadlock situations", measurementType = MeasurementType.TRENDSUP)
   public long getOverlapWithNotDeadlockAwareLockOwners() {
      return overlapWithNotDeadlockAwareLockOwners.get();
   }

   @ManagedAttribute(description = "Number of locally originated transactions that were interrupted as a deadlock situation was detected")
   @Metric(displayName = "Number of interrupted local transactions", measurementType = MeasurementType.TRENDSUP)
   public long getLocallyInterruptedTransactions() {
      return locallyInterruptedTransactions.get();
   }

   @ManagedAttribute(description = "Number of remote deadlocks detected")
   @Metric(displayName = "Number of detected remote deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedRemoteDeadlocks() {
      return detectedRemoteDeadlocks.get();
   }

   @ManagedAttribute (description = "Number of local detected deadlocks")
   @Metric(displayName = "Number of detected local deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getDetectedLocalDeadlocks() {
      return detectedLocalDeadlocks.get();
   }

   @ManagedAttribute (description = "Total number of local detected deadlocks")
   @Metric(displayName = "Number of total detected deadlocks", measurementType = MeasurementType.TRENDSUP)
   public long getTotalNumberOfDetectedDeadlocks() {
      return detectedRemoteDeadlocks.get() + detectedLocalDeadlocks.get();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      overlapWithNotDeadlockAwareLockOwners.set(0);
      locallyInterruptedTransactions.set(0);
      detectedRemoteDeadlocks.set(0);
      detectedLocalDeadlocks.set(0);
   }

}
