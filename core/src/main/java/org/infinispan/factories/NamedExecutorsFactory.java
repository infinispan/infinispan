package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.executors.LazyInitializingBlockingTaskAwareExecutorService;
import org.infinispan.executors.LazyInitializingExecutorService;
import org.infinispan.executors.LazyInitializingScheduledExecutorService;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;

import java.util.concurrent.*;

import static org.infinispan.factories.KnownComponentNames.*;

/**
 * A factory that specifically knows how to create named executors.
 *
 * @author Manik Surtani
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = {ExecutorService.class, Executor.class, ScheduledExecutorService.class,
                              BlockingTaskAwareExecutorService.class})
public class NamedExecutorsFactory extends NamedComponentFactory implements AutoInstantiableFactory {

   private ExecutorService notificationExecutor;
   private ExecutorService asyncTransportExecutor;
   private ExecutorService persistenceExecutor;
   private BlockingTaskAwareExecutorService remoteCommandsExecutor;
   private ScheduledExecutorService evictionExecutor;
   private ScheduledExecutorService asyncReplicationExecutor;
   private BlockingTaskAwareExecutorService totalOrderExecutor;

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType, String componentName) {
      try {
         // Construction happens only on startup of either CacheManager, or Cache, so
         // using synchronized protection does not have a great impact on app performance.
         if (componentName.equals(ASYNC_NOTIFICATION_EXECUTOR)) {
            synchronized (this) {
               if (notificationExecutor == null) {
                  notificationExecutor = createExecutorService(
                        globalConfiguration.listenerThreadPool(),
                        globalConfiguration, ASYNC_NOTIFICATION_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return (T) notificationExecutor;
         } else if (componentName.equals(PERSISTENCE_EXECUTOR)) {
            synchronized (this) {
               if (persistenceExecutor == null) {
                  persistenceExecutor = createExecutorService(
                        globalConfiguration.persistenceThreadPool(),
                        globalConfiguration, PERSISTENCE_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return (T) persistenceExecutor;
         } else if (componentName.equals(ASYNC_TRANSPORT_EXECUTOR)) {
            synchronized (this) {
               if (asyncTransportExecutor == null) {
                  asyncTransportExecutor = createExecutorService(
                        globalConfiguration.transport().transportThreadPool(),
                        globalConfiguration, ASYNC_TRANSPORT_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return (T) asyncTransportExecutor;
         } else if (componentName.equals(EVICTION_SCHEDULED_EXECUTOR)) {
            synchronized (this) {
               if (evictionExecutor == null) {
                  evictionExecutor = createExecutorService(
                        globalConfiguration.evictionThreadPool(),
                        globalConfiguration, EVICTION_SCHEDULED_EXECUTOR,
                        ExecutorServiceType.SCHEDULED);
               }
            }
            return (T) evictionExecutor;
         } else if (componentName.equals(ASYNC_REPLICATION_QUEUE_EXECUTOR)) {
            synchronized (this) {
               if (asyncReplicationExecutor == null) {
                  asyncReplicationExecutor = createExecutorService(
                        globalConfiguration.replicationQueueThreadPool(),
                        globalConfiguration, ASYNC_REPLICATION_QUEUE_EXECUTOR,
                        ExecutorServiceType.SCHEDULED);
               }
            }
            return (T) asyncReplicationExecutor;
         } else if (componentName.equals(REMOTE_COMMAND_EXECUTOR)) {
            synchronized (this) {
               if (remoteCommandsExecutor == null) {
                  remoteCommandsExecutor = createExecutorService(
                        globalConfiguration.transport().remoteCommandThreadPool(),
                        globalConfiguration, REMOTE_COMMAND_EXECUTOR,
                        ExecutorServiceType.BLOCKING);
               }
            }
            return (T) remoteCommandsExecutor;
         } else if (componentName.equals(TOTAL_ORDER_EXECUTOR)) {
            synchronized (this) {
               if (totalOrderExecutor == null) {
                  totalOrderExecutor = createExecutorService(
                        globalConfiguration.transport().totalOrderThreadPool(),
                        globalConfiguration, TOTAL_ORDER_EXECUTOR,
                        ExecutorServiceType.BLOCKING);
               }
            }
            return (T) totalOrderExecutor;
         } else {
            throw new CacheConfigurationException("Unknown named executor " + componentName);
         }
      } catch (CacheConfigurationException ce) {
         throw ce;
      } catch (Exception e) {
         throw new CacheConfigurationException("Unable to instantiate ExecutorFactory for named component " + componentName, e);
      }
   }

   @Stop(priority = 999)
   public void stop() {
      if (remoteCommandsExecutor != null) remoteCommandsExecutor.shutdownNow();
      if (notificationExecutor != null) notificationExecutor.shutdownNow();
      if (persistenceExecutor != null) persistenceExecutor.shutdownNow();
      if (asyncTransportExecutor != null) asyncTransportExecutor.shutdownNow();
      if (asyncReplicationExecutor != null) asyncReplicationExecutor.shutdownNow();
      if (evictionExecutor != null) evictionExecutor.shutdownNow();
      if (totalOrderExecutor != null) totalOrderExecutor.shutdownNow();
   }

   private <T extends ExecutorService> T createExecutorService(ThreadPoolConfiguration threadPoolConfiguration,
         GlobalConfiguration globalCfg, String componentName, ExecutorServiceType type) {
      ThreadFactory threadFactory;
      ThreadPoolExecutorFactory executorFactory;
      if (threadPoolConfiguration != null) {
         threadFactory = threadPoolConfiguration.threadFactory() != null
               ? threadPoolConfiguration.threadFactory()
               : createThreadFactoryWithDefaults(globalCfg, componentName);
         executorFactory = threadPoolConfiguration.threadPoolFactory() != null
               ? threadPoolConfiguration.threadPoolFactory()
               : createThreadPoolFactoryWithDefaults(componentName, type);
      } else {
         threadFactory = createThreadFactoryWithDefaults(globalCfg, componentName);
         executorFactory = createThreadPoolFactoryWithDefaults(componentName, type);
      }

      switch (type) {
         case SCHEDULED:
            return (T) new LazyInitializingScheduledExecutorService(executorFactory, threadFactory);
         case BLOCKING:
            return (T) new LazyInitializingBlockingTaskAwareExecutorService(
                  executorFactory, threadFactory, globalComponentRegistry.getTimeService());
         default:
            return (T) new LazyInitializingExecutorService(executorFactory, threadFactory);
      }
   }

   private ThreadFactory createThreadFactoryWithDefaults(GlobalConfiguration globalCfg, final String componentName) {
      // Use defaults
      return new DefaultThreadFactory(null,
            KnownComponentNames.getDefaultThreadPrio(componentName), DefaultThreadFactory.DEFAULT_PATTERN,
            globalCfg.transport().nodeName(), shortened(componentName));
   }

   private ThreadPoolExecutorFactory createThreadPoolFactoryWithDefaults(
         final String componentName, ExecutorServiceType type) {
      switch (type) {
         case SCHEDULED:
            return ScheduledThreadPoolExecutorFactory.create();
         default:
            int defaultQueueSize = KnownComponentNames.getDefaultQueueSize(componentName);
            int defaultMaxThreads = KnownComponentNames.getDefaultThreads(componentName);
            return BlockingThreadPoolExecutorFactory.create(defaultMaxThreads, defaultQueueSize);
      }
   }

   private enum ExecutorServiceType {
      DEFAULT, SCHEDULED, BLOCKING
   }

}
