package org.infinispan.configuration.global;

import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.FluentGlobalConfiguration;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Transport;

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

      // Implicitly enables stats
      legacy.globalJmxStatistics()
            .jmxDomain(config.globalJmxStatistics().domain())
            .mBeanServerLookup(config.globalJmxStatistics().mbeanServerLookup())
            .allowDuplicateDomains(config.globalJmxStatistics().allowDuplicateDomains())
            .cacheManagerName(config.globalJmxStatistics().cacheManagerName())
            .withProperties(config.globalJmxStatistics().properties());

      if (!config.globalJmxStatistics().enabled())
         legacy.globalJmxStatistics().disable();

      Marshaller marshaller = config.serialization().marshaller();
      if (marshaller instanceof org.infinispan.marshall.Marshaller)
      legacy.serialization()
         .marshallerClass(((org.infinispan.marshall.Marshaller)marshaller).getClass())
         .version(config.serialization().version());

      for (Entry<Integer, AdvancedExternalizer<?>> entry : config.serialization().advancedExternalizers().entrySet()) {
         AdvancedExternalizer<?> advancedExternalizer = entry.getValue();
         if (advancedExternalizer instanceof org.infinispan.marshall.AdvancedExternalizer)
            legacy.serialization().addAdvancedExternalizer(entry.getKey(), (org.infinispan.marshall.AdvancedExternalizer)advancedExternalizer);
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

      builder.globalJmxStatistics()
            .jmxDomain(legacy.getJmxDomain())
            .mBeanServerLookup(legacy.getMBeanServerLookupInstance())
            .allowDuplicateDomains(legacy.isAllowDuplicateDomains())
            .cacheManagerName(legacy.getCacheManagerName())
            .withProperties(legacy.getMBeanServerProperties());

      if (legacy.isExposeGlobalJmxStatistics())
         builder.globalJmxStatistics().enable();
      else
         builder.globalJmxStatistics().disable();

      builder.serialization()
         .marshaller(Util.<Marshaller>getInstance(legacy.getMarshallerClass(), legacy.getClassLoader()))
         .version(legacy.getMarshallVersion());

      for (AdvancedExternalizerConfig externalizerConfig : legacy.getExternalizers()) {
         org.infinispan.marshall.AdvancedExternalizer<?> ext = externalizerConfig.getAdvancedExternalizer();
         if (ext == null)
            ext = Util.getInstance(externalizerConfig.getExternalizerClass(),
                  legacy.getClassLoader());

         Integer id = externalizerConfig.getId();
         if (id != null)
            builder.serialization().addAdvancedExternalizer(id, ext);
         else
            builder.serialization().addAdvancedExternalizer(ext);
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

      return builder.build();
   }

}
