package org.infinispan.executors;

import java.util.concurrent.Executor;

/**
 * Class that can be used to guarantee that any operation submitted to the given executor will be run on the same
 * thread at a later point.
 */
public class LocalExecutorThreadLocal {
   private static final ThreadLocal<Executor> tlExecutor = new ThreadLocal<>();

   public static Executor localExecutor() {
      return tlExecutor.get();
   }

   public static void setLocalExecutor(Executor executor) {
      tlExecutor.set(executor);
   }
}
