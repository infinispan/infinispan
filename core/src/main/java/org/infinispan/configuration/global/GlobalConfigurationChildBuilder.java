package org.infinispan.configuration.global;

public interface GlobalConfigurationChildBuilder {
   TransportConfigurationBuilder transport();

   GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   SerializationConfigurationBuilder serialization();

   ExecutorFactoryConfigurationBuilder asyncListenerExecutor();

   ExecutorFactoryConfigurationBuilder persistenceExecutor();

   ExecutorFactoryConfigurationBuilder asyncTransportExecutor();
   
   ExecutorFactoryConfigurationBuilder remoteCommandsExecutor();
   
   ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor();

   ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor();

   ShutdownConfigurationBuilder shutdown();

   SiteConfigurationBuilder site();

   GlobalConfiguration build();
}