package org.infinispan.util.concurrent;

import java.util.concurrent.ExecutorService;

/**
 * Executor service that is aware of {@code BlockingRunnable} and only dispatch the runnable to a thread when it has low
 * (or no) probability of blocking the thread.
 * <p/>
 * However, it is not aware of the changes in the state so you must invoke {@link #checkForReadyTasks()} to notify
 * this that some runnable may be ready to be processed.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public interface BlockingTaskAwareExecutorService extends ExecutorService {

   /**
    * Executes the given command at some time in the future when the command is less probably to block a thread.
    *
    * @param runnable the command to execute
    */
   void execute(BlockingRunnable runnable);

   /**
    * It checks for tasks ready to be processed in this {@link ExecutorService}.
    *
    * The invocation is done asynchronously, so the invoker is never blocked.
    */
   void checkForReadyTasks();

}
