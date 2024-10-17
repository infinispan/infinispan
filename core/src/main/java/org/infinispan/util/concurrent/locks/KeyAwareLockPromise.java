package org.infinispan.util.concurrent.locks;

import org.infinispan.interceptors.InvocationStage;

/**
 * An extension of {@link LockPromise} that contains a key associated to the lock.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface KeyAwareLockPromise extends LockPromise {

   KeyAwareLockPromise NO_OP = new KeyAwareLockPromise() {
      @Override
      public void addListener(KeyAwareLockListener listener) {
         listener.onEvent(null, LockState.ACQUIRED);
      }

      public boolean isAvailable() {
         return true;
      }

      public void lock() {/*no-op*/}

      public void addListener(LockListener listener) {
         listener.onEvent(LockState.ACQUIRED);
      }

      @Override
      public InvocationStage toInvocationStage() {
         return InvocationStage.completedNullStage();
      }

      @Override
      public void completeExceptionally(LockState state) { }
   };

   /**
    * It adds the listener to this {@link LockPromise}.
    * <p>
    * The listener is invoked when the {@link LockPromise#isAvailable()} returns true. For more info, check {@link
    * KeyAwareLockListener}.
    *
    * @param listener the listener to add.
    */
   void addListener(KeyAwareLockListener listener);

   /**
    * Completes the promise exceptionally with the given lock state.
    * <p>
    * Exceptionally means the promise is canceled. Therefore, locks might fail during {@link #lock()} calls.
    * If the promise is already complete, this method has no effect.
    * </p>
    *
    * @param state: Exceptional state to complete. One of {@link LockState#ILLEGAL}, {@link LockState#DEADLOCKED}, or
    *             {@link LockState#TIMED_OUT}.
    */
   void completeExceptionally(LockState state);
}
