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

import static org.infinispan.factories.KnownComponentNames.*;

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
         if (componentName.equals(ASYNC_NOTIFICATION_EXECUTOR)) {
            return (T) buildAndConfigureExecutorService(globalConfiguration.getAsyncListenerExecutorFactoryClass(),
                                                        globalConfiguration.getAsyncListenerExecutorProperties(), componentName);
         } else if (componentName.equals(ASYNC_TRANSPORT_EXECUTOR)) {
            return (T) buildAndConfigureExecutorService(globalConfiguration.getAsyncTransportExecutorFactoryClass(),
                                                        globalConfiguration.getAsyncTransportExecutorProperties(), componentName);
         } else if (componentName.equals(EVICTION_SCHEDULED_EXECUTOR)) {
            return (T) buildAndConfigureScheduledExecutorService(globalConfiguration.getEvictionScheduledExecutorFactoryClass(),
                                                                 globalConfiguration.getEvictionScheduledExecutorProperties(), componentName);
         } else if (componentName.equals(ASYNC_REPLICATION_QUEUE_EXECUTOR)) {
            return (T) buildAndConfigureScheduledExecutorService(globalConfiguration.getReplicationQueueScheduledExecutorFactoryClass(),
                                                                 globalConfiguration.getReplicationQueueScheduledExecutorProperties(), componentName);
         } else {
            throw new ConfigurationException("Unknown named executor " + componentName);
         }
      } catch (ConfigurationException ce) {
         throw ce;
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate ExecutorFactory for named component " + componentName, e);
      }
   }

   private ExecutorService buildAndConfigureExecutorService(String factoryName, Properties p, String componentName) throws Exception {
      Properties props = new Properties(p); // defensive copy
      ExecutorFactory f = (ExecutorFactory) Util.getInstance(factoryName);
      setComponentName(componentName, props);
      setDefaultThreads(KnownComponentNames.getDefaultThreads(componentName), props);
      setDefaultThreadPrio(KnownComponentNames.getDefaultThreadPrio(componentName), props);
      return f.getExecutor(props);
   }

   private ScheduledExecutorService buildAndConfigureScheduledExecutorService(String factoryName, Properties p, String componentName) throws Exception {
      Properties props = new Properties(); // defensive copy
      if (p != null && !p.isEmpty()) props.putAll(p);
      ScheduledExecutorFactory f = (ScheduledExecutorFactory) Util.getInstance(factoryName);
      setComponentName(componentName, props);
      setDefaultThreadPrio(KnownComponentNames.getDefaultThreadPrio(componentName), props);
      return f.getScheduledExecutor(props);
   }

   private void setDefaultThreadPrio(int prio, Properties props) {
      if (!props.containsKey("threadPriority")) props.setProperty("threadPriority", "" + prio);
   }

   private void setDefaultThreads(int numThreads, Properties props) {
      if (!props.containsKey("maxThreads")) props.setProperty("maxThreads", "" + numThreads);
   }

   private void setComponentName(String cn, Properties p) {
      if (cn != null) p.setProperty("componentName", format(cn));
   }

   private String format(String cn) {
      int dotIndex = cn.lastIndexOf(".");
      int dotIndexPlusOne = dotIndex + 1;
      String cname = cn;
      if (dotIndexPlusOne == cn.length())
         cname = format(cn.substring(0, cn.length() - 1));
      else {
         if (dotIndex > -1 && cn.length() > dotIndexPlusOne) {
            cname = cn.substring(dotIndexPlusOne);
         }
         cname += "-thread";
      }
      return cname;
   }
}
