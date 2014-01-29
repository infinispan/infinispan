package org.infinispan.interceptors.distribution;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.statetransfer.StateTransferLock;
import org.jboss.logging.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
* A write synchronizer that allows for a single thread to run the L1 update while others can block until it is
* completed.  Also allows for someone to attempt to cancel the write to the L1.  If they are unable to, they should
* really wait until the L1 write has completed so they can guarantee their update will be ordered properly.
*
* @author wburns
* @since 6.0
*/
public class L1WriteSynchronizer {
   private static final Logger log = Logger.getLogger(L1WriteSynchronizer.class);
   private final L1WriteSync sync = new L1WriteSync();

   private final long l1Lifespan;
   private final DataContainer dc;
   private final StateTransferLock stateTransferLock;
   private final ClusteringDependentLogic cdl;

   public L1WriteSynchronizer(DataContainer dc, long l1Lifespan, StateTransferLock stateTransferLock,
                              ClusteringDependentLogic cdl) {
      this.dc = dc;
      this.l1Lifespan = l1Lifespan;
      this.stateTransferLock = stateTransferLock;
      this.cdl = cdl;
   }

   private static class L1WriteSync extends AbstractQueuedSynchronizer {
      private static final int READY = 0;
      private static final int RUNNING = 1;
      private static final int SKIP = 2;
      private static final int COMPLETED = 4;

      private Object result;
      private Throwable exception;

      /**
       * Implements AQS base acquire to succeed when completed
       */
      protected int tryAcquireShared(int ignore) {
         return getState() == COMPLETED ? 1 : -1;
      }

      /**
       * Implements AQS base release to always signal after setting
       * value
       */
      protected boolean tryReleaseShared(int ignore) {
         return true;
      }

      /**
       * Attempt to update the sync to signal that we want to update L1 with value
       * @return whether it should continue with running L1 update
       */
      boolean attemptUpdateToRunning() {
         // Multiple invocations should say it is marked as running
         if (getState() == RUNNING) {
            return true;
         }
         return compareAndSetState(READY, RUNNING);
      }

      /**
       * Attempt to update the sync to signal that we want to cancel the L1 update
       * @return whether the L1 run was skipped
       */
      boolean attemptToSkipFullRun() {
         // Multiple invocations should say it skipped
         if (getState() == SKIP) {
            return true;
         }
         return compareAndSetState(READY, SKIP);
      }

      Object innerGet() throws InterruptedException, ExecutionException {
         acquireSharedInterruptibly(0);
         if (exception != null) {
            throw new ExecutionException(exception);
         }
         return result;
      }

      Object innerGet(long time, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
         if (!tryAcquireSharedNanos(0, unit.toNanos(time))) {
            throw new TimeoutException();
         }
         if (exception != null) {
            throw new ExecutionException(exception);
         }
         return result;
      }

      void innerSet(Object value) {
         // This should never have to loop, but just in case :P
         for (;;) {
            int s = getState();
            if (s == COMPLETED) {
               return;
            }
            if (compareAndSetState(s, COMPLETED)) {
               result = value;
               releaseShared(0);
               return;
            }
         }
      }

      void innerException(Throwable t) {
         // This should never have to loop, but just in case :P
         for (;;) {
            int s = getState();
            if (s == COMPLETED) {
               return;
            }
            if (compareAndSetState(s, COMPLETED)) {
               exception = t;
               releaseShared(0);
               return;
            }
         }
      }
   }

   public Object get() throws InterruptedException, ExecutionException {
      return sync.innerGet();
   }

   public Object get(long time, TimeUnit unit) throws TimeoutException, InterruptedException, ExecutionException {
      return sync.innerGet(time, unit);
   }

   /**
    * Attempts to mark the L1 update to only retrieve the value and not to actually update the L1 cache.
    * If the L1 skipping is not successful, that means it is currently running, which means for consistency
    * any writes should wait until this update completes since the update doesn't acquire any locks
    * @return Whether or not it was successful in skipping L1 update
    */
   public boolean trySkipL1Update() {
      return sync.attemptToSkipFullRun();
   }

   public void retrievalEncounteredException(Throwable t) {
      sync.innerException(t);
   }

   /**
    * Attempts to the L1 update and set the value.  If the L1 update was marked as being skipped this will instead
    * just set the value to release blockers.
    * A null value can be provided which will not run the L1 update but will just alert other waiters that a null
    * was given.
    */
   public void runL1UpdateIfPossible(InternalCacheEntry ice) {
      Object value = null;
      try {
         if (ice != null) {
            value = ice.getValue();
            Object key;
            if (sync.attemptUpdateToRunning() && !dc.containsKey((key = ice.getKey()))) {
               // Acquire the transfer lock to ensure that we don't have a rehash and change to become an owner,
               // note we check the ownership in following if
               stateTransferLock.acquireSharedTopologyLock();
               try {
                  // Now we can update the L1 if there isn't a value already there and we haven't now become a write
                  // owner
                  if (!dc.containsKey(key) && !cdl.localNodeIsOwner(key)) {
                     log.tracef("Caching remotely retrieved entry for key %s in L1", key);
                     long lifespan = ice.getLifespan() < 0 ? l1Lifespan : Math.min(ice.getLifespan(), l1Lifespan);
                     // Make a copy of the metadata stored internally, adjust
                     // lifespan/maxIdle settings and send them a modification
                     Metadata newMetadata = ice.getMetadata().builder()
                           .lifespan(lifespan).maxIdle(-1).build();
                     dc.put(key, ice.getValue(), newMetadata);
                  } else {
                     log.tracef("Data container contained value after rehash for key %s", key);
                  }
               }
               finally {
                  stateTransferLock.releaseSharedTopologyLock();
               }
            }
         }
      }
      finally {
         sync.innerSet(value);
      }
   }
}
