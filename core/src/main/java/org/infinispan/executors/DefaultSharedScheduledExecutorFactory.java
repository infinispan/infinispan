package org.infinispan.executors;

import org.infinispan.util.TypedProperties;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default executor factory that creates a single scheduled executor and reuses it for subsequent calls to {@link #getScheduledExecutor(java.util.Properties)}.
 * <p />
 * Note that the executor is only shared amongst equivalent configurations (by checking that the Properties passed in are
 * equal).  What this means is two calls to {@link #getScheduledExecutor(java.util.Properties)} with 2 inequal
 * properties parameters will result in 2 separate executors being created.
 * <p />
 * The executors created are standard JDK executors, identical to those created by {@link DefaultExecutorFactory}.
 *
 *
 * @author Manik Surtani
 * @since 4.2
 */
public class DefaultSharedScheduledExecutorFactory extends AbstractSharedExecutorFactory<ScheduledExecutorService> implements ScheduledExecutorFactory {

   private final ScheduledExecutorFactory delegate = new DefaultScheduledExecutorFactory();

   @Override
   protected ScheduledExecutorService createService(Properties p) {
      return delegate.getScheduledExecutor(p);
   }

   @Override
   public ScheduledExecutorService getScheduledExecutor(Properties p) {
      return getOrCreateService(p);
   }
}
