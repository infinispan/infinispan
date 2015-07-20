package org.infinispan.util.concurrent.locks;

/**
 * A promise returned by {@link PendingLockManager}.
 * <p>
 * When a transaction need to wait for older topology transaction, this class allows it to check the state. If the
 * transaction does not need to wait, or all older transactions have finished or have timed out, the {@link #isReady()}
 * method returns {@code true}. Also, it allows the caller to add listeners to be notified when it is ready.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockPromise {

   PendingLockPromise NO_OP = new PendingLockPromise() {
      @Override
      public boolean isReady() {
         return true;
      }

      @Override
      public void addListener(PendingLockListener listener) {
         listener.onReady();
      }

      @Override
      public boolean hasTimedOut() {
         return false;
      }

      @Override
      public long getRemainingTimeout() {
         return Long.MAX_VALUE;
      }
   };

   /**
    * @return {@code true} when the transaction has finished the waiting.
    */
   boolean isReady();

   /**
    * Adds a listener to this promise.
    * <p>
    * The listener must be non-null and it is invoked only once. If {@link #isReady()} returns {@code true}, the {@code
    * listener} is immediately invoked in the invoker thread.
    *
    * @param listener the {@link PendingLockListener} to add.
    */
   void addListener(PendingLockListener listener);

   /**
    * @return {@code true} if the time out happened while waiting for older transactions.
    */
   boolean hasTimedOut();

   /**
    * @return the remaining timeout. It is zero when {@link #hasTimedOut()}  is {@code true}.
    */
   long getRemainingTimeout();

}
