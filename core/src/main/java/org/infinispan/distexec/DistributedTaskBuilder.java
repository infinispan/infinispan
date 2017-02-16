package org.infinispan.distexec;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * DistributedTaskBuilder is a factory interface for DistributedTask
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTaskBuilder<T> {

   /**
    * Provide relevant {@link Callable} for the {@link DistributedTask}
    *
    * @param callable
    *           for the DistribtuedTask being built
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> callable(Callable<T> callable);

   /**
    * Provide {@link DistributedTask} task timeout
    *
    * @param timeout
    *           for the task
    * @param tu
    *           {@link TimeUnit} for the task being built
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> timeout(long timeout, TimeUnit tu);

   /**
    * Provide {@link DistributedTaskExecutionPolicy} for the task being built
    *
    * @param policy
    *           DistributedTaskExecutionPolicy for the task
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> executionPolicy(DistributedTaskExecutionPolicy policy);

   /**
    * Provide {@link DistributedTaskFailoverPolicy} for the task being built
    *
    * @param policy
    *           DistributedTaskFailoverPolicy for the task
    * @return this DistributedTaskBuilder
    */
   DistributedTaskBuilder<T> failoverPolicy(DistributedTaskFailoverPolicy policy);

   /**
    * Completes creation of DistributedTask with the currently provided attributes of this
    * DistributedTaskBuilder
    *
    * @return the built task ready for use
    */
   DistributedTask<T> build();
}
