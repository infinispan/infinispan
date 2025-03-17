package org.infinispan.client.hotrod.impl.async;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

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
import org.infinispan.commons.ThreadGroups;
import org.infinispan.commons.executors.ExecutorFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Default implementation for {@link org.infinispan.commons.executors.ExecutorFactory} based on an {@link
 * ThreadPoolExecutor}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DefaultAsyncExecutorFactory implements ExecutorFactory {
   public static final String THREAD_NAME = "HotRod-client-async-pool";
   private static final Log log = LogFactory.getLog(DefaultAsyncExecutorFactory.class);

   private static final AtomicInteger factoryCounter = new AtomicInteger(0);

   @Override
   public ThreadPoolExecutor getExecutor(Properties p) {
      ConfigurationProperties cp = new ConfigurationProperties(p);
      int factoryIndex = DefaultAsyncExecutorFactory.factoryCounter.incrementAndGet();
      String threadNamePrefix = cp.getDefaultExecutorFactoryThreadNamePrefix();
      ThreadGroups.ISPNNonBlockingThreadGroup nonBlockingThreadGroup = ThreadGroups.NON_BLOCKING_GROUP;
      ThreadFactory tf = new DefaultThreadFactory(threadNamePrefix, true, Thread.NORM_PRIORITY, nonBlockingThreadGroup);

      log.debugf("Creating executor %s-%d", threadNamePrefix,  factoryIndex);
      return new ThreadPoolExecutor(cp.getDefaultExecutorFactoryPoolSize(), cp.getDefaultExecutorFactoryPoolSize(),
            0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), tf, (r, executor) -> {
         int poolSize = cp.getDefaultExecutorFactoryPoolSize();
         HOTROD.cannotCreateAsyncThread(poolSize);
         throw new RejectedExecutionException("Too few threads: " + poolSize);
      });
   }

}
