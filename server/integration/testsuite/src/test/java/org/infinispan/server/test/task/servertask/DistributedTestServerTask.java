package org.infinispan.server.test.task.servertask;


import java.util.Map;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/20/16
 * Time: 6:33 AM
 */
public class DistributedTestServerTask implements ServerTask {

   public static final String NAME = "serverTask777792";
   public static final String EXCEPTION_MESSAGE = "Intentionally Thrown Exception";
   private TaskContext taskContext;

   @Override
   public Object call() {
      Map<String, Boolean> parameters = (Map<String, Boolean>) taskContext.getParameters().get();
      if (parameters == null || parameters.isEmpty()) {
         return System.getProperty("jboss.node.name");
      } else {
         throw new RuntimeException(EXCEPTION_MESSAGE);
      }
   }

   @Override
   public void setTaskContext(TaskContext taskContext) {
      this.taskContext = taskContext;
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
