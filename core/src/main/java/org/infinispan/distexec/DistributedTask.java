package org.infinispan.distexec;

import java.util.concurrent.Callable;

/**
 * DistributedTask describes all relevant attributes of a distributed task, most importantly its
 * execution policy, fail over policy and its timeout.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTask<T> {

   /**
    * Returns timeout for the execution of this task
    *
    * @return task timeout
    */
   long timeout();

   /**
    * Returns custom {@link DistributedTaskExecutionPolicy} for this task
    *
    * @return task DistributedTaskExecutionPolicy
    */
   DistributedTaskExecutionPolicy getTaskExecutionPolicy();

   /**
    * Returns custom {@link DistributedTaskFailoverPolicy}  for this task
    *
    * @return
    */
   DistributedTaskFailoverPolicy getTaskFailoverPolicy();

   /**
    * Returns {@link Callable} for this task
    *
    * @return task callable
    */
   Callable<T> getCallable();

}
