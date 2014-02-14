package org.infinispan.executors;

import org.infinispan.commons.util.TypedProperties;

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

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      TypedProperties tp = new TypedProperties(p);
      final String threadNamePrefix = p.getProperty("threadNamePrefix", p.getProperty("componentName", "Thread"));
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);

      return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, "Scheduled-" + threadNamePrefix + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            th.setContextClassLoader(DefaultScheduledExecutorFactory.class.getClassLoader());
            th.setPriority(threadPrio);
            return th;
         }
      });
   }
}
