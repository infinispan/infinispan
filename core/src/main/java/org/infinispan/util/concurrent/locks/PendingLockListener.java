package org.infinispan.util.concurrent.locks;

/**
 * A listener that is invoked when {@link PendingLockPromise} is ready.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockListener {

   /**
    * Invoked when {@link PendingLockPromise} is ready.
    */
   void onReady();

}
