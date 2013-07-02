package org.infinispan.configuration.global;

public interface GlobalConfigurationChildBuilder {
   TransportConfigurationBuilder transport();

   GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   SerializationConfigurationBuilder serialization();

   ExecutorFactoryConfigurationBuilder asyncListenerExecutor();

   ExecutorFactoryConfigurationBuilder asyncTransportExecutor();
   
   ExecutorFactoryConfigurationBuilder remoteCommandsExecutor();
   
   ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor();

   ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor();

   ShutdownConfigurationBuilder shutdown();

   SiteConfigurationBuilder site();

   GlobalConfiguration build();
}