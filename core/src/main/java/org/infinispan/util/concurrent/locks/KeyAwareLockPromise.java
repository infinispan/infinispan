package org.infinispan.util.concurrent.locks;

import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.SyncInvocationStage;
import org.infinispan.util.concurrent.TimeoutException;

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

      public void lock() throws InterruptedException, TimeoutException {/*no-op*/}

      public void addListener(LockListener listener) {
         listener.onEvent(LockState.ACQUIRED);
      }

      @Override
      public InvocationStage toInvocationStage() {
         return new SyncInvocationStage();
      }

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

}
