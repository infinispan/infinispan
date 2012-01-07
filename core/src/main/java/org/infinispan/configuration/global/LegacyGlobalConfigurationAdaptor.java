package org.infinispan.configuration.global;

import java.util.Map.Entry;

import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.FluentGlobalConfiguration;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.Util;

public class LegacyGlobalConfigurationAdaptor {
   
   public org.infinispan.config.GlobalConfiguration adapt(GlobalConfiguration config) {
      
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
         .marshallerClass(config.serialization().marshallerClass())
         .version(config.serialization().version());
      
      for (Entry<Integer, AdvancedExternalizer<?>> entry : config.serialization().advancedExternalizers().entrySet()) {
         legacy.serialization().addAdvancedExternalizer(entry.getKey(), entry.getValue());
      }
      
      legacy.asyncTransportExecutor()
         .factory(config.asyncTransportExecutor().factory().getClass())
         .withProperties(config.asyncTransportExecutor().properties());
      
      legacy.asyncListenerExecutor()
         .factory(config.asyncListenerExecutor().factory().getClass())
         .withProperties(config.asyncListenerExecutor().properties());
      
      legacy.evictionScheduledExecutor()
         .factory(config.evictionScheduledExecutor().factory().getClass())
         .withProperties(config.asyncListenerExecutor().properties());
      
      legacy.replicationQueueScheduledExecutor()
         .factory(config.replicationQueueScheduledExecutor().factory().getClass())
         .withProperties(config.replicationQueueScheduledExecutor().properties());
      
      legacy.shutdown().hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.valueOf(config.shutdown().hookBehavior().name()));
      
      return legacy.build();
   }
   
   public org.infinispan.configuration.global.GlobalConfiguration adapt(org.infinispan.config.GlobalConfiguration legacy) {
      
      // Handle the case that null is passed in
      if (legacy == null)
         return null;
      
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();

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
      
      if (legacy.isExposeGlobalJmxStatistics()) {
         builder.globalJmxStatistics()
            .jmxDomain(legacy.getJmxDomain())
            .mBeanServerLookup(legacy.getMBeanServerLookupInstance())
            .allowDuplicateDomains(legacy.isAllowDuplicateDomains())
            .cacheManagerName(legacy.getCacheManagerName());
      }
      else
         builder.globalJmxStatistics().disable();
         
      
      builder.serialization()
         .marshallerClass(Util.<Marshaller>loadClass(legacy.getMarshallerClass(), legacy.getClassLoader()))
         .version(legacy.getMarshallVersion());
      
      for (AdvancedExternalizerConfig externalizerConfig : legacy.getExternalizers()) {
         builder.serialization().addAdvancedExternalizer(externalizerConfig.getAdvancedExternalizer());
      }
      
      builder.asyncTransportExecutor()
         .factory(Util.<ExecutorFactory>getInstance(legacy.getAsyncTransportExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getAsyncTransportExecutorProperties());
      
      builder.asyncListenerExecutor()
         .factory(Util.<ExecutorFactory>getInstance(legacy.getAsyncListenerExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getAsyncListenerExecutorProperties());
      
      builder.evictionScheduledExecutor()
         .factory(Util.<ScheduledExecutorFactory>getInstance(legacy.getEvictionScheduledExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getAsyncListenerExecutorProperties());
      
      builder.replicationQueueScheduledExecutor()
         .factory(Util.<ScheduledExecutorFactory>getInstance(legacy.getReplicationQueueScheduledExecutorFactoryClass(), legacy.getClassLoader()))
         .withProperties(legacy.getReplicationQueueScheduledExecutorProperties());
      
      builder.shutdown().hookBehavior(ShutdownHookBehavior.valueOf(legacy.getShutdownHookBehavior().name()));
      
      return builder.build();
   }

}
