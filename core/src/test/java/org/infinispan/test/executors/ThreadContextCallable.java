package org.infinispan.test.executors;

import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.ThreadContext;
import org.jboss.logging.NDC;

/**
 * Wraps a Runnable or Callable to preserve Log4j2 ThreadContext (MDC) and JBoss Logging NDC
 * across executor thread boundaries. This is used in tests to ensure thread names and cache
 * context are visible in log files.
 *
 * @author William Burns
 * @since 16.2
 */
final class ThreadContextCallable<V> implements Callable<V> {
   private final Callable<V> delegate;
   private final Map<String, String> context;
   private final String ndcStack;

   private ThreadContextCallable(Callable<V> delegate, Map<String, String> context, String ndcStack) {
      this.delegate = delegate;
      this.context = context;
      this.ndcStack = ndcStack;
   }

   /**
    * Wraps a Callable with the current ThreadContext and NDC.
    */
   static <V> Callable<V> wrap(Callable<V> callable) {
      if (callable == null || callable instanceof ThreadContextCallable) {
         return callable;
      }
      Map<String, String> context = ThreadContext.getImmutableContext();
      String ndcStack = NDC.get();
      if (context.isEmpty() && ndcStack == null) {
         return callable;
      }
      return new ThreadContextCallable<>(callable, context, ndcStack);
   }

   /**
    * Wraps a Runnable with the current ThreadContext and NDC.
    */
   static Runnable wrap(Runnable runnable) {
      if (runnable == null || runnable instanceof ThreadContextRunnable) {
         return runnable;
      }
      Map<String, String> context = ThreadContext.getImmutableContext();
      String ndcStack = NDC.get();
      if (context.isEmpty() && ndcStack == null) {
         return runnable;
      }
      return new ThreadContextRunnable(runnable, context, ndcStack);
   }

   @Override
   public V call() throws Exception {
      Map<String, String> previousContext = ThreadContext.getImmutableContext();
      String previousNdc = NDC.get();

      ThreadContext.clearAll();
      NDC.clear();

      if (!context.isEmpty()) {
         ThreadContext.putAll(context);
      }
      if (ndcStack != null) {
         NDC.push(ndcStack);
      }

      try {
         return delegate.call();
      } finally {
         ThreadContext.clearAll();
         NDC.clear();

         if (!previousContext.isEmpty()) {
            ThreadContext.putAll(previousContext);
         }
         if (previousNdc != null) {
            NDC.push(previousNdc);
         }
      }
   }

   private static final class ThreadContextRunnable implements Runnable {
      private final Runnable delegate;
      private final Map<String, String> context;
      private final String ndcStack;

      ThreadContextRunnable(Runnable delegate, Map<String, String> context, String ndcStack) {
         this.delegate = delegate;
         this.context = context;
         this.ndcStack = ndcStack;
      }

      @Override
      public void run() {
         Map<String, String> previousContext = ThreadContext.getImmutableContext();
         String previousNdc = NDC.get();

         ThreadContext.clearAll();
         NDC.clear();

         if (!context.isEmpty()) {
            ThreadContext.putAll(context);
         }
         if (ndcStack != null) {
            NDC.push(ndcStack);
         }

         try {
            delegate.run();
         } finally {
            ThreadContext.clearAll();
            NDC.clear();

            if (!previousContext.isEmpty()) {
               ThreadContext.putAll(previousContext);
            }
            if (previousNdc != null) {
               NDC.push(previousNdc);
            }
         }
      }
   }
}
