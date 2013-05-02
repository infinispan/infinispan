/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.executors.LazyInitializingBlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.LazyInitializingExecutorService;
import org.infinispan.executors.LazyInitializingScheduledExecutorService;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Stop;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

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
   private ExecutorService remoteCommandsExecutor;
   private ScheduledExecutorService evictionExecutor;
   private ScheduledExecutorService asyncReplicationExecutor;
   private BlockingTaskAwareExecutorService totalOrderExecutor;

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType, String componentName) {
      try {
         String nodeName = globalConfiguration.transport().nodeName();

         // Construction happens only on startup of either CacheManager, or Cache, so
         // using synchronized protection does not have a great impact on app performance.
         if (componentName.equals(ASYNC_NOTIFICATION_EXECUTOR)) {
            synchronized (this) {
               if (notificationExecutor == null) {
                  notificationExecutor = buildAndConfigureExecutorService(
                        globalConfiguration.asyncListenerExecutor().factory(),
                        globalConfiguration.asyncListenerExecutor().properties(), componentName, nodeName);
               }
            }
            return (T) notificationExecutor;
         } else if (componentName.equals(ASYNC_TRANSPORT_EXECUTOR)) {
            synchronized (this) {
               if (asyncTransportExecutor == null) {
                  asyncTransportExecutor = buildAndConfigureExecutorService(
                        globalConfiguration.asyncTransportExecutor().factory(),
                        globalConfiguration.asyncTransportExecutor().properties(), componentName, nodeName);
               }
            }
            return (T) asyncTransportExecutor;
         } else if (componentName.equals(EVICTION_SCHEDULED_EXECUTOR)) {
            synchronized (this) {
               if (evictionExecutor == null) {
                  evictionExecutor = buildAndConfigureScheduledExecutorService(
                        globalConfiguration.evictionScheduledExecutor().factory(),
                        globalConfiguration.evictionScheduledExecutor().properties(), componentName, nodeName);
               }
            }
            return (T) evictionExecutor;
         } else if (componentName.equals(ASYNC_REPLICATION_QUEUE_EXECUTOR)) {
            synchronized (this) {
               if (asyncReplicationExecutor == null) {
                  asyncReplicationExecutor = buildAndConfigureScheduledExecutorService(
                        globalConfiguration.replicationQueueScheduledExecutor().factory(),
                        globalConfiguration.replicationQueueScheduledExecutor().properties(), componentName,
                        nodeName);
               }
            }
            return (T) asyncReplicationExecutor;
         } else if (componentName.equals(REMOTE_COMMAND_EXECUTOR)) {
            synchronized (this) {
               if (remoteCommandsExecutor == null) {
                  remoteCommandsExecutor = buildAndConfigureExecutorService(
                        globalConfiguration.remoteCommandsExecutor().factory(),
                        globalConfiguration.remoteCommandsExecutor().properties(), componentName, nodeName);
               }
            }
            return (T) remoteCommandsExecutor;
         } else if (componentName.equals(TOTAL_ORDER_EXECUTOR)) {
            synchronized (this) {
               if (totalOrderExecutor == null) {
                  totalOrderExecutor = buildAndConfigureBlockingTaskAwareExecutorService(
                        globalConfiguration.totalOrderExecutor().factory(),
                        globalConfiguration.totalOrderExecutor().properties(), componentName, nodeName);
               }
            }
            return (T) totalOrderExecutor;
         } else {
            throw new ConfigurationException("Unknown named executor " + componentName);
         }
      } catch (ConfigurationException ce) {
         throw ce;
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate ExecutorFactory for named component " + componentName, e);
      }
   }

   @Stop(priority = 999)
   public void stop() {
      if (remoteCommandsExecutor != null) remoteCommandsExecutor.shutdownNow();
      if (notificationExecutor != null) notificationExecutor.shutdownNow();
      if (asyncTransportExecutor != null) asyncTransportExecutor.shutdownNow();
      if (asyncReplicationExecutor != null) asyncReplicationExecutor.shutdownNow();
      if (evictionExecutor != null) evictionExecutor.shutdownNow();
      if (totalOrderExecutor != null) totalOrderExecutor.shutdownNow();
   }

   private ExecutorService buildAndConfigureExecutorService(ExecutorFactory f, Properties p,
                                                            String componentName, String nodeName) throws Exception {
      Properties props = new Properties(p); // defensive copy
      if (p != null && !p.isEmpty()) props.putAll(p);
      setThreadSuffix(nodeName, props);
      setComponentName(componentName, props);
      setDefaultThreads(KnownComponentNames.getDefaultThreads(componentName), props);
      setDefaultThreadPrio(KnownComponentNames.getDefaultThreadPrio(componentName), props);
      setDefaultQueueSize(KnownComponentNames.getDefaultQueueSize(componentName), props);
      return new LazyInitializingExecutorService(f, props);
   }

   private ScheduledExecutorService buildAndConfigureScheduledExecutorService(ScheduledExecutorFactory f,
                                                                              Properties p, String componentName,
                                                                              String nodeName) throws Exception {
      Properties props = new Properties(); // defensive copy
      if (p != null && !p.isEmpty()) props.putAll(p);
      setThreadSuffix(nodeName, props);
      setComponentName(componentName, props);
      setDefaultThreadPrio(KnownComponentNames.getDefaultThreadPrio(componentName), props);
      return new LazyInitializingScheduledExecutorService(f, props);
   }

   private BlockingTaskAwareExecutorService buildAndConfigureBlockingTaskAwareExecutorService(ExecutorFactory f,
                                                                                              Properties p, String componentName,
                                                                                              String nodeName) throws Exception {
      Properties props = new Properties(); // defensive copy
      if (p != null && !p.isEmpty()) props.putAll(p);
      setThreadSuffix(nodeName, props);
      setComponentName(componentName, props);
      setDefaultThreads(KnownComponentNames.getDefaultThreads(componentName), props);
      setDefaultThreadPrio(KnownComponentNames.getDefaultThreadPrio(componentName), props);
      setDefaultQueueSize(KnownComponentNames.getDefaultQueueSize(componentName), props);
      return new LazyInitializingBlockingTaskAwareExecutorService(f, props, globalComponentRegistry.getTimeService());
   }

   private void setThreadSuffix(String nodeName, Properties props) {
      if (nodeName != null && !nodeName.isEmpty()) {
         props.setProperty("threadNameSuffix", ',' + nodeName);
      }
   }

   private void setDefaultQueueSize(int queueSize, Properties props) {
      if (!props.containsKey("queueSize")) props.setProperty("queueSize", String.valueOf(queueSize));
   }

   private void setDefaultThreadPrio(int prio, Properties props) {
      if (!props.containsKey("threadPriority")) props.setProperty("threadPriority", String.valueOf(prio));
   }

   private void setDefaultThreads(int numThreads, Properties props) {
      if (!props.containsKey("maxThreads")) props.setProperty("maxThreads", String.valueOf(numThreads));
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
