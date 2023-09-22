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
   private static final org.infinispan.commons.spi.ThreadCreator INSTANCE = getInstance();

   public static boolean useVirtualThreads() {
      return Boolean.getBoolean("org.infinispan.threads.virtual");
   }

   private static org.infinispan.commons.spi.ThreadCreator getInstance() {
      try {
         if (useVirtualThreads()) {
            org.infinispan.commons.spi.ThreadCreator instance = Util.getInstance("org.infinispan.commons.jdk21.ThreadCreatorImpl", ThreadCreator.class.getClassLoader());
            Log.CONTAINER.infof("Virtual threads support enabled");
            return instance;
         }
      } catch (Throwable t) {
         Log.CONTAINER.debugf("Could not initialize virtual threads", t);
      }
      return new ThreadCreatorImpl();
   }


   public static Thread createThread(ThreadGroup threadGroup, Runnable target, boolean useVirtualThread) {
      return INSTANCE.createThread(threadGroup, target, useVirtualThread);
   }

   public static Optional<ExecutorService> createBlockingExecutorService() {
      return INSTANCE.newVirtualThreadPerTaskExecutor();
   }

   static class ThreadCreatorImpl implements org.infinispan.commons.spi.ThreadCreator {

      @Override
      public Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight) {
         return new Thread(threadGroup, target);
      }

      @Override
      public Optional<ExecutorService> newVirtualThreadPerTaskExecutor() {
         return Optional.empty();
      }
   }
}
