package org.infinispan.commons.jdk21;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ThreadCreatorImpl implements org.infinispan.commons.spi.ThreadCreator {

   public Thread createThread(ThreadGroup group, Runnable runnable, boolean useVirtualThreads) {
      if (useVirtualThreads) {
         return Thread.ofVirtual().unstarted(runnable);
      } else {
         return new Thread(group, runnable);
      }
   }

   @Override
   public Optional<ExecutorService> newVirtualThreadPerTaskExecutor() {
      return Optional.of(Executors.newVirtualThreadPerTaskExecutor());
   }

}
