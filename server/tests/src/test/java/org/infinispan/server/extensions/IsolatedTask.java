package org.infinispan.server.extensions;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskInstantiationMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class IsolatedTask implements ServerTask<Integer> {
   private final AtomicInteger invocationCount = new AtomicInteger(0);

   @Override
   public void setTaskContext(TaskContext taskContext) {
      // do nothing
   }

   @Override
   public TaskInstantiationMode getInstantiationMode() {
      return TaskInstantiationMode.ISOLATED;
   }

   @Override
   public Integer call() {
      return invocationCount.addAndGet(1);
   }

   @Override
   public String getName() {
      return "isolated";
   }
}
