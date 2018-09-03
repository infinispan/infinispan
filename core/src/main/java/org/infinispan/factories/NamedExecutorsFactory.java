package org.infinispan.factories;

import static org.infinispan.factories.KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.STATE_TRANSFER_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.getDefaultThreadPrio;
import static org.infinispan.factories.KnownComponentNames.shortened;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ScheduledThreadPoolExecutorFactory;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.executors.LazyInitializingBlockingTaskAwareExecutorService;
import org.infinispan.executors.LazyInitializingExecutorService;
import org.infinispan.executors.LazyInitializingScheduledExecutorService;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;

/**
 * A factory that specifically knows how to create named executors.
 *
 * @author Manik Surtani
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(names = {ASYNC_TRANSPORT_EXECUTOR, ASYNC_NOTIFICATION_EXECUTOR, PERSISTENCE_EXECUTOR, ASYNC_OPERATIONS_EXECUTOR,
                             EXPIRATION_SCHEDULED_EXECUTOR, REMOTE_COMMAND_EXECUTOR, STATE_TRANSFER_EXECUTOR, TIMEOUT_SCHEDULE_EXECUTOR})
public class NamedExecutorsFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private ExecutorService notificationExecutor;
   private ExecutorService asyncTransportExecutor;
   private ExecutorService persistenceExecutor;
   private BlockingTaskAwareExecutorService remoteCommandsExecutor;
   private ScheduledExecutorService expirationExecutor;
   private ExecutorService stateTransferExecutor;
   private ExecutorService asyncOperationsExecutor;
   private ScheduledExecutorService timeoutExecutor;

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      try {
         // Construction happens only on startup of either CacheManager, or Cache, so
         // using synchronized protection does not have a great impact on app performance.
         if (componentName.equals(ASYNC_NOTIFICATION_EXECUTOR)) {
            synchronized (this) {
               if (notificationExecutor == null) {
                  notificationExecutor = createExecutorService(
                        globalConfiguration.listenerThreadPool(),
                        ASYNC_NOTIFICATION_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return notificationExecutor;
         } else if (componentName.equals(PERSISTENCE_EXECUTOR)) {
            synchronized (this) {
               if (persistenceExecutor == null) {
                  persistenceExecutor = createExecutorService(
                        globalConfiguration.persistenceThreadPool(),
                        PERSISTENCE_EXECUTOR,
                        ExecutorServiceType.SCHEDULED);
               }
            }
            return persistenceExecutor;
         } else if (componentName.equals(ASYNC_TRANSPORT_EXECUTOR)) {
            synchronized (this) {
               if (asyncTransportExecutor == null) {
                  asyncTransportExecutor = createExecutorService(
                        globalConfiguration.transport().transportThreadPool(),
                        ASYNC_TRANSPORT_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return asyncTransportExecutor;
         } else if (componentName.equals(EXPIRATION_SCHEDULED_EXECUTOR)) {
            synchronized (this) {
               if (expirationExecutor == null) {
                  expirationExecutor = createExecutorService(
                        globalConfiguration.expirationThreadPool(),
                        EXPIRATION_SCHEDULED_EXECUTOR,
                        ExecutorServiceType.SCHEDULED);
               }
            }
            return expirationExecutor;
         } else if (componentName.equals(REMOTE_COMMAND_EXECUTOR)) {
            synchronized (this) {
               if (remoteCommandsExecutor == null) {
                  remoteCommandsExecutor = createExecutorService(
                        globalConfiguration.transport().remoteCommandThreadPool(),
                        REMOTE_COMMAND_EXECUTOR,
                        ExecutorServiceType.BLOCKING);
               }
            }
            return remoteCommandsExecutor;
         } else if (componentName.equals(STATE_TRANSFER_EXECUTOR)) {
            synchronized (this) {
               if (stateTransferExecutor == null) {
                  stateTransferExecutor = createExecutorService(
                        globalConfiguration.stateTransferThreadPool(),
                        STATE_TRANSFER_EXECUTOR,
                        ExecutorServiceType.DEFAULT);
               }
            }
            return stateTransferExecutor;
         } else if (componentName.equals(ASYNC_OPERATIONS_EXECUTOR)) {
            synchronized (this) {
               if (asyncOperationsExecutor == null) {
                  asyncOperationsExecutor = createExecutorService(
                        globalConfiguration.asyncThreadPool(),
                        ASYNC_OPERATIONS_EXECUTOR, ExecutorServiceType.DEFAULT);
               }
            }
            return asyncOperationsExecutor;
         } else if (componentName.endsWith(TIMEOUT_SCHEDULE_EXECUTOR)) {
            synchronized (this) {
               if (timeoutExecutor == null) {
                  timeoutExecutor = createExecutorService(null, TIMEOUT_SCHEDULE_EXECUTOR, ExecutorServiceType.SCHEDULED);
               }
            }
            return timeoutExecutor;
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
      // TODO Dan: awaitTermination()?
      if (remoteCommandsExecutor != null) remoteCommandsExecutor.shutdownNow();
      if (notificationExecutor != null) notificationExecutor.shutdownNow();
      if (persistenceExecutor != null) persistenceExecutor.shutdownNow();
      if (asyncTransportExecutor != null) asyncTransportExecutor.shutdownNow();
      if (expirationExecutor != null) expirationExecutor.shutdownNow();
      if (stateTransferExecutor != null) stateTransferExecutor.shutdownNow();
      if (timeoutExecutor != null) timeoutExecutor.shutdownNow();
      if (asyncOperationsExecutor != null) asyncOperationsExecutor.shutdownNow();
   }

   @SuppressWarnings("unchecked")
   private <T extends ExecutorService> T createExecutorService(ThreadPoolConfiguration threadPoolConfiguration,
                                                               String componentName, ExecutorServiceType type) {
      ThreadFactory threadFactory;
      ThreadPoolExecutorFactory executorFactory;
      if (threadPoolConfiguration != null) {
         threadFactory = threadPoolConfiguration.threadFactory() != null
               ? threadPoolConfiguration.threadFactory()
               : createThreadFactoryWithDefaults(globalConfiguration, componentName);
         executorFactory = threadPoolConfiguration.threadPoolFactory() != null
               ? threadPoolConfiguration.threadPoolFactory()
               : createThreadPoolFactoryWithDefaults(componentName, type);
      } else {
         threadFactory = createThreadFactoryWithDefaults(globalConfiguration, componentName);
         executorFactory = createThreadPoolFactoryWithDefaults(componentName, type);
      }

      switch (type) {
         case SCHEDULED:
            return (T) new LazyInitializingScheduledExecutorService(executorFactory, threadFactory);
         case BLOCKING:
            final String controllerName = "Controller-" + shortened(componentName) + "-" +
                  globalConfiguration.transport().nodeName();
            return (T) new LazyInitializingBlockingTaskAwareExecutorService(executorFactory, threadFactory,
                                                                        globalComponentRegistry.getTimeService(),
                                                                        controllerName);
         default:
            return (T) new LazyInitializingExecutorService(executorFactory, threadFactory);
      }
   }

   private ThreadFactory createThreadFactoryWithDefaults(GlobalConfiguration globalCfg, final String componentName) {
      // Use defaults
      return new DefaultThreadFactory(null, getDefaultThreadPrio(componentName), DefaultThreadFactory.DEFAULT_PATTERN,
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
