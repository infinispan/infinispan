package org.infinispan.util.concurrent;

/**
 * A special Runnable (for the particular case of Total Order) that is only sent to a thread when it is ready to be
 * executed without blocking the thread
 * <p>
 * Use case: - in Total Order, when the prepare is delivered, the runnable blocks waiting for the previous conflicting
 * transactions to be finished. In a normal executor service, this will take a thread and that thread will be blocked.
 * This way, the runnable waits on the queue and not in the Thread
 * <p>
 * Used in {@code org.infinispan.util.concurrent.BlockingTaskAwareExecutorService}
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface BlockingRunnable extends Runnable {

   /**
    * @return true if this Runnable is ready to be executed without blocking
    */
   boolean isReady();

}
