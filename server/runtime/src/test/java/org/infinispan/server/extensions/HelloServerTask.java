package org.infinispan.server.extensions;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

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

   public static JavaArchive artifact() {
      JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "hello-server-task.jar");
      jar.addClass(HelloServerTask.class);
      jar.addAsServiceProvider(ServerTask.class, HelloServerTask.class);
      return jar;
   }

}
