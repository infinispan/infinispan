package org.infinispan.client.hotrod.impl.async;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.util.TypedProperties;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation for {@link org.infinispan.executors.ExecutorFactory} based on an {@link ThreadPoolExecutor}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "HotRod-client-async-pool";
   public static final AtomicInteger counter = new AtomicInteger(0);
   private int poolSize = 10;
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
      TypedProperties tp = TypedProperties.toTypedProperties(props);
      poolSize = tp.getIntProperty(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, 10);
      queueSize = tp.getIntProperty(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE, 100000);
   }
}
