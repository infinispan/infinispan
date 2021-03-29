package org.infinispan.server.extensions;

import java.util.ArrayList;
import java.util.Collection;

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

      // if we're dealing with a Collections of greetees we'll greet them individually
      if (greetee instanceof Collection) {
         ArrayList<String> messages = new ArrayList<>();
         for (Object o : (Collection<?>) greetee) {
            messages.add(greet(o));
         }
         return messages;
      }

      return greet(greetee);
   }

   private String greet(Object greetee) {
      return greetee == null ? "Hello world" : "Hello " + greetee;
   }

   @Override
   public String getName() {
      return "hello";
   }
}
