package org.infinispan.server.infinispan.task;

import org.infinispan.tasks.TaskExecutionMode;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/28/16
 * Time: 9:32 AM
 */
public class ServerTaskRunnerFactory {

   private static final LocalServerTaskRunner localRunner = new LocalServerTaskRunner();
   private static final DistributedServerTaskRunner distributedRunner = new DistributedServerTaskRunner();

   public ServerTaskRunner getRunner(TaskExecutionMode executionMode) {
      switch (executionMode) {
         case ONE_NODE:
            return localRunner;
         case ALL_NODES:
            return distributedRunner;
      }
      return null;
   }
}
