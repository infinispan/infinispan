package org.infinispan.executors;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates scheduled executors using the JDK Executors service
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultScheduledExecutorFactory implements ScheduledExecutorFactory {
   final static AtomicInteger counter = new AtomicInteger(0);

   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      final String threadNamePrefix = p.getProperty("threadNamePrefix", "ScheduledThread");
      return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            return th;
         }
      });
   }
}
