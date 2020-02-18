package org.infinispan.server.extensions;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HelloServerTask implements ServerTask {
   private TaskContext taskContext;

   @Override
   public void setTaskContext(TaskContext taskContext) {
      this.taskContext = taskContext;
   }

   @Override
   public Object call() {
      Object greetee = taskContext.getParameters().get().get("greetee");
      return greetee == null ? "Hello world" : "Hello " + greetee;
   }

   @Override
   public String getName() {
      return "hello";
   }

}
