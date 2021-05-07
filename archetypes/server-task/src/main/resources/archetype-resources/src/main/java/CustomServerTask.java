package ${package};

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

import java.nio.charset.StandardCharsets;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class CustomServerTask<Object> implements ServerTask<Object> {
   private TaskContext ctx;

   /**
    * Allows access to execution context information including task parameters, cache references on
    * which tasks are executed, and so on. In most cases, implementations store this information
    * locally in all cases as it is set on each node.
    */
   @Override
   public void setTaskContext(TaskContext ctx) {
      this.ctx = ctx;
   }

   /**
    * Computes a result. This method is defined in the java.util.concurrent.Callable interface and
    * is invoked with server tasks.
    */
   @Override
   public Object call() {
      // TODO add task logic here.
      return null;
   }

   /**
    * Returns unique names for tasks. Clients invoke tasks with these names.
    */
   @Override
   public String getName() {
      // TODO update to return the name of the task
      return null;
   }

   /**
    * Returns the execution mode for tasks.
    * 
    * TaskExecutionMode.ONE_NODE only the node that handles the request executes the script.
    * Although scripts can still invoke clustered operations.
    * 
    * TaskExecutionMode.ALL_NODES Infinispan uses clustered executors to run scripts across nodes.
    * For example, server tasks that invoke stream processing need to be executed on a single node
    * because stream processing is distributed to all nodes.
    */
   @Override
   public TaskExecutionMode getExecutionMode() {
      // TODO update
      return TaskExecutionMode.ONE_NODE;
   }
}
