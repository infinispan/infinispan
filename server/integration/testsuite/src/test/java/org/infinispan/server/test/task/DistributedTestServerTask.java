package org.infinispan.server.test.task;


import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

import java.util.Set;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 6:33 AM
 */
public class DistributedTestServerTask implements ServerTask {

   public static final String NAME = "serverTask777792";

   @Override
   public Object call() {
      return System.getProperty("jboss.node.name");
   }

   @Override
   public void setTaskContext(TaskContext taskContext) {
   }

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }
}
