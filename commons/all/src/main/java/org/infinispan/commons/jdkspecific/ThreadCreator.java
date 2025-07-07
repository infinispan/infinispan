package org.infinispan.commons.jdkspecific;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ThreadCreator {

   private static boolean useVirtualThreads = Boolean.parseBoolean(System.getProperty("org.infinispan.threads.virtual", "true"));
   private static org.infinispan.commons.spi.ThreadCreator INSTANCE = getInstance(useVirtualThreads);

   public static void useVirtualThreads(boolean useVirtualThreads) {
      if (useVirtualThreads != useVirtualThreads()) {
         ThreadCreator.useVirtualThreads = useVirtualThreads;
         INSTANCE = getInstance(useVirtualThreads);
      }
   }

   public static boolean useVirtualThreads() {
      return useVirtualThreads;
   }

   public static boolean isVirtualThreadsEnabled() {
      return INSTANCE.isVirtualThreadsEnabled();
   }

   public static Thread createThread(ThreadGroup threadGroup, Runnable target, boolean useVirtualThread) {
      return INSTANCE.createThread(threadGroup, target, useVirtualThread);
   }

   public static Optional<ExecutorService> createBlockingExecutorService() {
      return INSTANCE.newVirtualThreadPerTaskExecutor();
   }

   private static org.infinispan.commons.spi.ThreadCreator getInstance(boolean useVirtualThreads) {
      try {
         if (useVirtualThreads) {
            org.infinispan.commons.spi.ThreadCreator instance = Util.getInstance("org.infinispan.commons.jdk21.ThreadCreatorImpl", ThreadCreator.class.getClassLoader());
            Log.CONTAINER.virtualThreadSupport("enabled");
            return instance;
         } else {
            Log.CONTAINER.virtualThreadSupport("disabled");
         }
      } catch (Throwable t) {
         Log.CONTAINER.virtualThreadSupport("unavailable");
         Log.CONTAINER.debugf("Could not initialize virtual threads", t);
      }
      return new ThreadCreatorImpl();
   }

   public static boolean isVirtual(Thread thread) {
      return INSTANCE.isVirtual(thread);
   }

   private static class ThreadCreatorImpl implements org.infinispan.commons.spi.ThreadCreator {

      @Override
      public Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight) {
         return new Thread(threadGroup, target);
      }

      @Override
      public Optional<ExecutorService> newVirtualThreadPerTaskExecutor() {
         return Optional.empty();
      }

      @Override
      public boolean isVirtual(Thread thread) {
         return false;
      }

      @Override
      public boolean isVirtualThreadsEnabled() {
         return false;
      }
   }
}
