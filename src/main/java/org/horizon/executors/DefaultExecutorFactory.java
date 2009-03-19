package org.horizon.executors;

import org.horizon.util.TypedProperties;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor factory that creates executors using the JDK Executors service.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class DefaultExecutorFactory implements ExecutorFactory {
   public ExecutorService getExecutor(Properties p) {
      TypedProperties tp = TypedProperties.toTypedProperties(p);
      int maxThreads = tp.getIntProperty("maxThreads", 1);
      int queueSize = tp.getIntProperty("queueSize", 100000);
      final String threadNamePrefix = tp.getProperty("threadNamePrefix", "Thread");
      final AtomicInteger counter = new AtomicInteger(0);

      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            return new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement());
         }
      };

      return new ThreadPoolExecutor(maxThreads, maxThreads,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(queueSize),
                                    tf);
   }
}
