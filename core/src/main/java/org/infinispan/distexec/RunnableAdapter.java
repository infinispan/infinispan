package org.infinispan.distexec;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Runnable adapter for distributed executor service
 * Any RunnableAdapter refactoring might break CDI
 */
public final class RunnableAdapter<T> implements Callable<T>, Serializable {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = 6629286923873531028L;

   protected Runnable task;
   protected T result;

   protected RunnableAdapter() {
   }

   protected RunnableAdapter(Runnable task, T result) {
      this.task = task;
      this.result = result;
   }

   public Runnable getTask() {
      return task;
   }

   @Override
   public T call() {
      task.run();
      return result;
   }
}
