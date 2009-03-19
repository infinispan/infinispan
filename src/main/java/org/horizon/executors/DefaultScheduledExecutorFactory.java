package org.horizon.executors;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates scheduled executors using the JDK Executors service
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class DefaultScheduledExecutorFactory implements ScheduledExecutorFactory {
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      final String threadNamePrefix = p.getProperty("threadNamePrefix", "ScheduledThread");
      final AtomicInteger counter = new AtomicInteger(0);
      return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         public Thread newThread(Runnable r) {
            return new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement());
         }
      });
   }
}
