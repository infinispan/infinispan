package org.infinispan.client.hotrod.impl.async;

import org.infinispan.executors.ExecutorFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "Hotrod-client-async-pool";
   public static final AtomicInteger counter = new AtomicInteger(0);
   private int poolSize = 1;
   private int queueSize = 100000;

   @Override
   public ExecutorService getExecutor(Properties p) {
      readParams(p);
      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread th = new Thread(r, THREAD_NAME + "-" + counter.getAndIncrement());
            th.setDaemon(true);
            return th;
         }
      };

      return new ThreadPoolExecutor(poolSize, poolSize,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(queueSize),
                                    tf);
   }

   private void readParams(Properties props) {
      if (props.contains("default-executor-factory.poolSize")) {
         poolSize = Integer.parseInt(props.getProperty("default-executor-factory.poolSize"));
      }
      if (props.contains("default-executor-factory.queueSize")) {
         queueSize = Integer.parseInt("default-executor-factory.queueSize");
      }
   }
}
