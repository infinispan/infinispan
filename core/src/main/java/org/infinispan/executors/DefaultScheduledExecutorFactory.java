package org.infinispan.executors;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.TypedProperties;

/**
 * Creates scheduled executors using the JDK Executors service
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 4.0
 */
public class DefaultScheduledExecutorFactory implements SecurityAwareScheduledExecutorFactory {
   final static AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      return getScheduledExecutor(p, null);
   }

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p, AccessControlContext context) {
      TypedProperties tp = new TypedProperties(p);
      final String threadNamePrefix = p.getProperty("threadNamePrefix", p.getProperty("componentName", "Thread"));
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);
      final AccessControlContext acc = context;
      return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

         public Thread createThread(Runnable r) {
            Thread th = new Thread(r, "Scheduled-" + threadNamePrefix + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            th.setPriority(threadPrio);
            return th;
         }

         @Override
         public Thread newThread(Runnable r) {
            final Runnable runnable = r;

            if (System.getSecurityManager() != null && acc != null) {
               return AccessController.doPrivileged(new PrivilegedAction<Thread>() {
                  @Override
                  public Thread run() {
                     return createThread(runnable);
                  }
               }, acc);
            } else {
               return createThread(runnable);
            }
         }
      });

   }
}
