package org.infinispan.distexec;

import org.infinispan.remoting.transport.Address;

/**
 * DistributedTaskFailoverPolicy allows pluggable fail over target selection for a failed remotely
 * executed distributed task.
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public interface DistributedTaskFailoverPolicy {

   /**
    * As parts of distributively executed task can fail due to the task itself throwing an exception
    * or it can be an Infinispan system caused failure (e.g node failed or left cluster during task
    * execution etc).
    *
    * @param context
    *           the FailoverContext of the failed execution
    * @return result the Address of the Infinispan node selected for fail over execution
    */
   Address failover(FailoverContext context);

   /**
    * Maximum number of fail over attempts permitted by this DistributedTaskFailoverPolicy
    *
    * @return max number of fail over attempts
    */
   int maxFailoverAttempts();
}
