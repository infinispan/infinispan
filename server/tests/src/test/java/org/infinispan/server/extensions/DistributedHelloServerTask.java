package org.infinispan.server.extensions;

import java.util.ArrayList;
import java.util.Collection;

import javax.security.auth.Subject;

import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class DistributedHelloServerTask implements ServerTask<Object> {

   private TaskContext taskContext;

   @Override
   public void setTaskContext(TaskContext taskContext) {
      this.taskContext = taskContext;
   }

   @Override
   public Object call() {
      Address address = taskContext.getCacheManager().getAddress();
      Object greetee = taskContext.getParameters().get().get("greetee");

      if (greetee == null) {
         if (taskContext.getSubject().isPresent()) {
            Subject subject = Security.getSubject();
            greetee = subject.getPrincipals().iterator().next().getName();
            if (!greetee.equals(Security.getSubject().getPrincipals().iterator().next().getName())) {
               throw new RuntimeException("Subjects do not match");
            }
         } else {
            greetee = "world";
         }
      }

      // if we're dealing with a Collections of greetees we'll greet them individually
      if (greetee instanceof Collection) {
         ArrayList<String> messages = new ArrayList<>();
         for (Object o : (Collection<?>) greetee) {
            messages.add(greet(o, address));
         }
         return messages;
      }
      return greet(greetee, address);
   }

   private String greet(Object greetee, Address address) {
      return String.format("Hello %s from %s", greetee, address);
   }

   @Override
   public TaskExecutionMode getExecutionMode() {
      return TaskExecutionMode.ALL_NODES;
   }

   @Override
   public String getName() {
      return "dist-hello";
   }
}
