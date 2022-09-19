package org.infinispan.executors;

import java.security.AccessControlContext;
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
import org.infinispan.factories.threads.BlockingThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadFactory;

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
      String blocking = tp.getProperty("blocking");
      ThreadGroup threadGroup;
      if (blocking == null) {
         threadGroup = Thread.currentThread().getThreadGroup();
      } else {
         threadGroup = Boolean.parseBoolean(blocking) ? new BlockingThreadFactory.ISPNBlockingThreadGroup(threadNamePrefix + "-group") :
               new NonBlockingThreadFactory.ISPNNonBlockingThreadGroup(threadNamePrefix + "-group");
      }
      BlockingQueue<Runnable> queue = queueSize == 0 ? new SynchronousQueue<>()
            : new LinkedBlockingQueue<>(queueSize);
      ThreadFactory tf = new ThreadFactory() {

         private Thread createThread(Runnable r) {
            String threadName = threadNamePrefix + "-" + counter.getAndIncrement() + threadNameSuffix;
            Thread th = new Thread(threadGroup, r, threadName);
            th.setDaemon(true);
            th.setPriority(threadPrio);
            return th;
         }

         @Override
         public Thread newThread(Runnable r) {
            return createThread(r);
         }
      };

      return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS, queue, tf,
            new ThreadPoolExecutor.CallerRunsPolicy());
   }
}
