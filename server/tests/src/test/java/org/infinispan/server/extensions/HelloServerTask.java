package org.infinispan.server.extensions;

import java.util.ArrayList;
import java.util.Collection;

import javax.security.auth.Subject;

import org.infinispan.security.Security;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HelloServerTask implements ServerTask<Object> {

   private static final ThreadLocal<TaskContext> taskContext = new ThreadLocal<>();

   @Override
   public void setTaskContext(TaskContext ctx) {
      taskContext.set(ctx);
   }

   @Override
   public Object call() {
      TaskContext ctx = taskContext.get();
      Object greetee = ctx.getParameters().get().get("greetee");

      if (greetee == null) {
         if (ctx.getSubject().isPresent()) {
            Subject subject = ctx.getSubject().get();
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
            messages.add(greet(o));
         }
         return messages;
      }

      return greet(greetee);
   }

   private String greet(Object greetee) {
      return "Hello " + greetee;
   }

   @Override
   public String getName() {
      return "hello";
   }
}
