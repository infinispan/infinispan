package org.infinispan.configuration.global;

import java.util.Map.Entry;

import org.infinispan.config.FluentGlobalConfiguration;
import org.infinispan.marshall.AdvancedExternalizer;

public class LegacyGlobalConfigurationAdaptor {
   
   public org.infinispan.config.GlobalConfiguration adapt(GlobalConfiguration config) {
      
      FluentGlobalConfiguration legacy = new org.infinispan.config.GlobalConfiguration(config.getClassLoader()).fluent();

      legacy.transport()
         .clusterName(config.getTransport().getClusterName())
         .machineId(config.getTransport().getMachineId())
         .rackId(config.getTransport().getRackId())
         .siteId(config.getTransport().getSiteId())
         .strictPeerToPeer(config.getTransport().isStrictPeerToPeer())
         .distributedSyncTimeout(config.getTransport().getDistributedSyncTimeout())
         .transportClass(config.getTransport().getTransport().getClass())
         .nodeName(config.getTransport().getNodeName())
         .withProperties(config.getTransport().getProperties());
      
      if (config.getGlobalJmxStatistics().isEnabled()) {
         legacy.globalJmxStatistics()
            .jmxDomain(config.getGlobalJmxStatistics().getDomain())
            .mBeanServerLookup(config.getGlobalJmxStatistics().getMBeanServerLookup())
            .allowDuplicateDomains(config.getGlobalJmxStatistics().isAllowDuplicateDomains())
            .cacheManagerName(config.getGlobalJmxStatistics().getCacheManagerName())
            .withProperties(config.getGlobalJmxStatistics().getProperties());
      }
      else
         legacy.globalJmxStatistics().disable();
         
      
      legacy.serialization()
         .marshallerClass(config.getSerialization().getMarshallerClass())
         .version(config.getSerialization().getVersion());
      
      for (Entry<Integer, AdvancedExternalizer<?>> entry : config.getSerialization().getAdvancedExternalizers().entrySet()) {
         legacy.serialization().addAdvancedExternalizer(entry.getValue());
      }
      
      legacy.asyncTransportExecutor()
         .factory(config.getAsyncTransportExecutor().getFactory().getClass())
         .withProperties(config.getAsyncTransportExecutor().getProperties());
      
      legacy.asyncListenerExecutor()
         .factory(config.getAsyncListenerExecutor().getFactory().getClass())
         .withProperties(config.getAsyncListenerExecutor().getProperties());
      
      legacy.evictionScheduledExecutor()
         .factory(config.getEvictionScheduledExecutor().getFactory().getClass())
         .withProperties(config.getAsyncListenerExecutor().getProperties());
      
      legacy.replicationQueueScheduledExecutor()
         .factory(config.getReplicationQueueScheduledExecutor().getFactory().getClass())
         .withProperties(config.getReplicationQueueScheduledExecutor().getProperties());
      
      legacy.shutdown().hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.valueOf(config.getShutdown().getHookBehavior().name()));
      
      return legacy.build();
   }

}
