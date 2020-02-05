package org.infinispan.util.concurrent;

/**
 * A special Runnable that is only sent to a thread when it is ready to be
 * executed without blocking the thread
 * <p/>
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
