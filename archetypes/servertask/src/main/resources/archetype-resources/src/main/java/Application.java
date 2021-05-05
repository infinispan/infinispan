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
public class Application<T> implements ServerTask<T> {
   private TaskContext ctx;

   /**
    * Allows access to execution context information including task parameters, cache references on
    * which tasks are executed, and so on. In most cases, implementations store this information
    * locally and use it when tasks are actually executed.
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
   public T call() {
       return null;
   }
   
   /**
    * Returns unique names for tasks. Clients invoke tasks with these names.
    */

   @Override
   public String getName() {
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
      return null;
   }
}
