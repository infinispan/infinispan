package org.infinispan.util.concurrent.locks.deadlock;

import java.util.Collection;
import java.util.Objects;

import org.infinispan.util.concurrent.locks.DeadlockDetection;
import org.infinispan.util.concurrent.locks.KeyAwareLockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A listener for lock events.
 * <p>
 * This listener is responsible for initializing the probing mechanism once a lock changes ownership. After receiving an
 * {@link LockState#ACQUIRED} event, the listener starts the deadlock detection algorithm for every lock pending to the same key.
 * </p>
 *
 * <p>
 * The listener registers to a pending promise, i.e., when a key is not immediately locked. Therefore, once it receives
 * an event, the previous owner has released the lock to the pending transactions. The pending owners must probe the holder
 * after every ownership change to guarantee progress in the deadlock detection. A single key can have multiple listeners attached.
 * </p>
 *
 * @see KeyAwareLockListener
 * @author José Bolina
 */
public final class DeadlockDetectionLockListener implements KeyAwareLockListener {

   private static final Log log = LogFactory.getLog(DeadlockDetectionLockListener.class);

   private final LockManager lockManager;
   private final DistributedDeadlockDetection ddl;

   public DeadlockDetectionLockListener(LockManager lockManager, DeadlockDetection detection) {
      this.lockManager = lockManager;
      this.ddl = detection instanceof DistributedDeadlockDetection enabled ? enabled : null;
   }

   @Override
   public void onEvent(Object key, LockState state) {
      if (ddl == null || state != LockState.ACQUIRED) return;

      Object owner = lockManager.getOwner(key);
      Collection<Object> pending = lockManager.getPendingOwners(key);

      log.tracef("key %s acquired by %s, pending (%s)", key, owner, pending);
      for (Object p : pending) {
         if (Objects.equals(p, owner)) continue;
         ddl.initializeDeadlockDetection(p, owner);
      }

      // Make sure the local transactions also start probing.
      ddl.probeAllLocalTransactions();
   }
}
