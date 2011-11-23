package org.infinispan.configuration.global;

import java.util.Map.Entry;

import org.infinispan.api.marshall.AdvancedExternalizer;
import org.infinispan.config.FluentGlobalConfiguration;

public class LegacyGlobalConfigurationAdaptor {
   
   public org.infinispan.config.GlobalConfiguration adapt(GlobalConfiguration config) {
      
      FluentGlobalConfiguration legacy = new org.infinispan.config.GlobalConfiguration(config.classLoader()).fluent();

      legacy.transport()
         .clusterName(config.transport().clusterName())
         .machineId(config.transport().machineId())
         .rackId(config.transport().rackId())
         .siteId(config.transport().siteId())
         .strictPeerToPeer(config.transport().strictPeerToPeer())
         .distributedSyncTimeout(config.transport().distributedSyncTimeout())
         .transportClass(config.transport().transport().getClass())
         .nodeName(config.transport().nodeName())
         .withProperties(config.transport().properties());
      
      if (config.globalJmxStatistics().enabled()) {
         legacy.globalJmxStatistics()
            .jmxDomain(config.globalJmxStatistics().domain())
            .mBeanServerLookup(config.globalJmxStatistics().mbeanServerLookup())
            .allowDuplicateDomains(config.globalJmxStatistics().allowDuplicateDomains())
            .cacheManagerName(config.globalJmxStatistics().cacheManagerName())
            .withProperties(config.globalJmxStatistics().properties());
      }
      else
         legacy.globalJmxStatistics().disable();
         
      
      legacy.serialization()
         .marshallerClass(config.serialization().marshallerClass())
         .version(config.serialization().version());
      
      for (Entry<Integer, AdvancedExternalizer<?>> entry : config.serialization().advancedExternalizers().entrySet()) {
         legacy.serialization().addAdvancedExternalizer(entry.getValue());
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

}
