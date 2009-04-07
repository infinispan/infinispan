package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.Util;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory that specifically knows how to create named executors.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = {ExecutorService.class, Executor.class, ScheduledExecutorService.class})
public class NamedExecutorsFactory extends NamedComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType, String componentName) {
      try {
         if (componentName.equals(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR)) {
            return (T) buildAndConfigureExecutorService(globalConfiguration.getAsyncListenerExecutorFactoryClass(),
                                                        globalConfiguration.getAsyncListenerExecutorProperties());
         } else if (componentName.equals(KnownComponentNames.ASYNC_SERIALIZATION_EXECUTOR)) {
            return (T) buildAndConfigureExecutorService(globalConfiguration.getAsyncSerializationExecutorFactoryClass(),
                                                        globalConfiguration.getAsyncSerializationExecutorProperties());
         } else if (componentName.equals(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR)) {
            return (T) buildAndConfigureScheduledExecutorService(globalConfiguration.getEvictionScheduledExecutorFactoryClass(),
                                                                 globalConfiguration.getEvictionScheduledExecutorProperties());
         } else if (componentName.equals(KnownComponentNames.ASYNC_REPLICATION_QUEUE_EXECUTOR)) {
            return (T) buildAndConfigureScheduledExecutorService(globalConfiguration.getReplicationQueueScheduledExecutorFactoryClass(),
                                                                 globalConfiguration.getReplicationQueueScheduledExecutorProperties());
         } else {
            throw new ConfigurationException("Unknown named executor " + componentName);
         }
      } catch (ConfigurationException ce) {
         throw ce;
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate ExecutorFactory for named component " + componentName, e);
      }
   }

   private ExecutorService buildAndConfigureExecutorService(String factoryName, Properties props) throws Exception {
      ExecutorFactory f = (ExecutorFactory) Util.getInstance(factoryName);
      return f.getExecutor(props);
   }

   private ScheduledExecutorService buildAndConfigureScheduledExecutorService(String factoryName, Properties props) throws Exception {
      ScheduledExecutorFactory f = (ScheduledExecutorFactory) Util.getInstance(factoryName);
      return f.getScheduledExecutor(props);
   }
}
