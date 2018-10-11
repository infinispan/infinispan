package org.infinispan.client.hotrod.impl.async;

import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.executors.ExecutorFactory;

/**
 * Default implementation for {@link org.infinispan.commons.executors.ExecutorFactory} based on an {@link
 * ThreadPoolExecutor}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "HotRod-client-async-pool";
   public static final AtomicInteger counter = new AtomicInteger(0);
   private static final Log log = LogFactory.getLog(DefaultAsyncExecutorFactory.class);

   @Override
   public ThreadPoolExecutor getExecutor(Properties p) {
      ConfigurationProperties cp = new ConfigurationProperties(p);
      final String threadNamePrefix = cp.getDefaultExecutorFactoryThreadNamePrefix();
      final String threadNameSuffix = cp.getDefaultExecutorFactoryThreadNameSuffix();
      ThreadFactory tf = r -> {
         Thread th = new Thread(r, threadNamePrefix + "-" + counter.getAndIncrement() + threadNameSuffix);
         th.setDaemon(true);
         return th;
      };

      return new ThreadPoolExecutor(cp.getDefaultExecutorFactoryPoolSize(), cp.getDefaultExecutorFactoryPoolSize(),
            0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), tf, (r, executor) -> {
         int poolSize = cp.getDefaultExecutorFactoryPoolSize();
         log.cannotCreateAsyncThread(poolSize);
         throw new RejectedExecutionException("Too few threads: " + poolSize);
      });
   }
}
