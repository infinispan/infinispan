package org.infinispan.executors;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.executors.SecurityAwareExecutorFactory;
import org.infinispan.commons.util.TypedProperties;

/**
 * Default executor factory that creates executors using the JDK Executors service.
 *
 * @author Manik Surtani
 * @author Tristan Tarrant
 * @since 4.0
 */
public class DefaultExecutorFactory implements SecurityAwareExecutorFactory {
   private final AtomicInteger counter = new AtomicInteger(0);

   @Override
   public ExecutorService getExecutor(Properties p) {
      return getExecutor(p, null);
   }

   @Override
   public ExecutorService getExecutor(Properties p, final AccessControlContext context) {
      TypedProperties tp = TypedProperties.toTypedProperties(p);
      int maxThreads = tp.getIntProperty("maxThreads", 1);
      int queueSize = tp.getIntProperty("queueSize", 100000);
      int coreThreads = queueSize == 0 ? 1 : tp.getIntProperty("coreThreads", maxThreads);
      long keepAliveTime = tp.getLongProperty("keepAliveTime", 60000);
      final int threadPrio = tp.getIntProperty("threadPriority", Thread.MIN_PRIORITY);
      final String threadNamePrefix = tp.getProperty("threadNamePrefix", tp.getProperty("componentName", "Thread"));
      final String threadNameSuffix = tp.getProperty("threadNameSuffix", "");
      BlockingQueue<Runnable> queue = queueSize == 0 ? new SynchronousQueue<Runnable>()
            : new LinkedBlockingQueue<Runnable>(queueSize);
      ThreadFactory tf = new ThreadFactory() {

         private Thread createThread(Runnable r) {
            String threadName = threadNamePrefix + "-" + counter.getAndIncrement() + threadNameSuffix;
            Thread th = new Thread(r, threadName);
            th.setDaemon(true);
            th.setPriority(threadPrio);
            return th;
         }

         @Override
         public Thread newThread(Runnable r) {
            final Runnable runnable = r;
            final AccessControlContext acc;
            if (System.getSecurityManager() != null && (acc = context) != null) {
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
      };

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS, queue, tf,
            new ThreadPoolExecutor.CallerRunsPolicy());
   }
}
