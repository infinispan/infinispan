/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.FluentGlobalConfiguration;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.Util;

import java.util.Map.Entry;

@SuppressWarnings("deprecation")
public class LegacyGlobalConfigurationAdaptor {
   private LegacyGlobalConfigurationAdaptor() {
      // Hide constructor
   }

   public static org.infinispan.config.GlobalConfiguration adapt(GlobalConfiguration config) {
      
      // Handle the case that null is passed in
      if (config == null)
         return null;
      
      FluentGlobalConfiguration legacy = new org.infinispan.config.GlobalConfiguration(config.classLoader()).fluent();

      legacy.transport()
         .clusterName(config.transport().clusterName())
         .machineId(config.transport().machineId())
         .rackId(config.transport().rackId())
         .siteId(config.transport().siteId())
         .strictPeerToPeer(config.transport().strictPeerToPeer())
         .distributedSyncTimeout(config.transport().distributedSyncTimeout())
         .nodeName(config.transport().nodeName())
         .withProperties(config.transport().properties());
      
      if (config.transport().transport() != null)
         legacy.transport().transportClass(config.transport().transport().getClass());
      
      if (config.globalJmxStatistics().enabled()) {
         legacy.globalJmxStatistics()
            .jmxDomain(config.globalJmxStatistics().domain())
            .mBeanServerLookup(config.globalJmxStatistics().mbeanServerLookup())
            .allowDuplicateDomains(config.globalJmxStatistics().allowDuplicateDomains())
            .cacheManagerName(config.globalJmxStatistics().cacheManagerName())
            .withProperties(config.globalJmxStatistics().properties());
      }
         
      
      legacy.serialization()
         .marshallerClass(config.serialization().marshaller().getClass())
         .version(config.serialization().version());
      
      for (Entry<Integer, AdvancedExternalizer<?>> entry : config.serialization().advancedExternalizers().entrySet()) {
         legacy.serialization().addAdvancedExternalizer(entry.getKey(), entry.getValue());
      }

      legacy.serialization().classResolver(config.serialization().classResolver());
      
      legacy.asyncTransportExecutor()
         .factory(config.asyncTransportExecutor().factory().getClass())
         .withProperties(config.asyncTransportExecutor().properties());
      
      legacy.asyncListenerExecutor()
         .factory(config.asyncListenerExecutor().factory().getClass())
         .withProperties(config.asyncListenerExecutor().properties());
      
      legacy.evictionScheduledExecutor()
         .factory(config.evictionScheduledExecutor().factory().getClass())
         .withProperties(config.evictionScheduledExecutor().properties());
      
      legacy.replicationQueueScheduledExecutor()
         .factory(config.replicationQueueScheduledExecutor().factory().getClass())
         .withProperties(config.replicationQueueScheduledExecutor().properties());
      
      legacy.shutdown().hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.valueOf(config.shutdown().hookBehavior().name()));
      
      legacy.totalOrderExecutor()
            .factory(config.totalOrderExecutor().factory().getClass())
            .withProperties(config.totalOrderExecutor().properties());

      return legacy.build();
   }
   
   public static org.infinispan.configuration.global.GlobalConfiguration adapt(org.infinispan.config.GlobalConfiguration legacy) {
      
      // Handle the case that null is passed in
      if (legacy == null)
         return null;
      
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();

      if (legacy.getTransportClass() != null) {
         builder.transport()
            .clusterName(legacy.getClusterName())
            .machineId(legacy.getMachineId())
            .rackId(legacy.getRackId())
            .siteId(legacy.getSiteId())
            .strictPeerToPeer(legacy.isStrictPeerToPeer())
            .distributedSyncTimeout(legacy.getDistributedSyncTimeout())
            .transport(Util.<Transport>getInstance(legacy.getTransportClass(), legacy.getClassLoader()))
            .nodeName(legacy.getTransportNodeName())
            .withProperties(legacy.getTransportProperties());
      }

      if (legacy.isExposeGlobalJmxStatistics()) {
         builder.globalJmxStatistics().enable()
            .jmxDomain(legacy.getJmxDomain())
            .mBeanServerLookup(legacy.getMBeanServerLookupInstance())
            .allowDuplicateDomains(legacy.isAllowDuplicateDomains())
            .cacheManagerName(legacy.getCacheManagerName());
      }
      else
         builder.globalJmxStatistics().disable();
         
      
      builder.serialization()
         .marshaller(Util.<Marshaller>getInstance(legacy.getMarshallerClass(), legacy.getClassLoader()))
         .version(legacy.getMarshallVersion());
      
      for (AdvancedExternalizerConfig externalizerConfig : legacy.getExternalizers()) {
         builder.serialization().addAdvancedExternalizer(externalizerConfig.getId(), externalizerConfig.getAdvancedExternalizer());
      }

      builder.serialization().classResolver(legacy.getClassResolver());
      
      builder.asyncTransportExecutor()
         .factory(Util.<ExecutorFactory>getInstance(legacy.getAsyncTransportExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getAsyncTransportExecutorProperties());
      
      builder.asyncListenerExecutor()
         .factory(Util.<ExecutorFactory>getInstance(legacy.getAsyncListenerExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getAsyncListenerExecutorProperties());
      
      builder.evictionScheduledExecutor()
         .factory(Util.<ScheduledExecutorFactory>getInstance(legacy.getEvictionScheduledExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getEvictionScheduledExecutorProperties());
      
      builder.replicationQueueScheduledExecutor()
         .factory(Util.<ScheduledExecutorFactory>getInstance(legacy.getReplicationQueueScheduledExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getReplicationQueueScheduledExecutorProperties());
      
      builder.shutdown().hookBehavior(ShutdownHookBehavior.valueOf(legacy.getShutdownHookBehavior().name()));
      
      builder.totalOrderExecutor()
            .factory(Util.<ExecutorFactory>getInstance(legacy.getTotalOrderExecutorFactorClass(), legacy.getClassLoader()))
            .withProperties(legacy.getTotalOrderExecutorProperties());

      return builder.build();
   }

}
