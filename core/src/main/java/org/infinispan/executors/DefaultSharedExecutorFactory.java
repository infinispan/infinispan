package org.infinispan.executors;

import org.infinispan.util.TypedProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor factory that creates a single executor and reuses it for subsequent calls to {@link #getExecutor(java.util.Properties)}.
 * <p />
 * Note that the executor is only shared amongst equivalent configurations (by checking that the Properties passed in are
 * equal).  What this means is two calls to {@link #getExecutor(java.util.Properties)} with 2 inequal 
 * properties parameters will result in 2 separate executors being created.
 * <p />
 * The executors created are standard JDK executors, identical to those created by {@link DefaultExecutorFactory}.
 *
 * @author Manik Surtani
 * @since 4.2
 */
public class DefaultSharedExecutorFactory extends AbstractSharedExecutorFactory<ExecutorService> implements ExecutorFactory {
   private final ExecutorFactory delegate = new DefaultExecutorFactory();

   @Override
   protected ExecutorService createService(Properties p) {
      return delegate.getExecutor(p);
   }

   @Override
   public ExecutorService getExecutor(Properties p) {
      return getOrCreateService(p);
   }
}
