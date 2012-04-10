package org.infinispan.configuration.global;

public interface GlobalConfigurationChildBuilder {
   TransportConfigurationBuilder transport();

   GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   SerializationConfigurationBuilder serialization();

   ExecutorFactoryConfigurationBuilder asyncListenerExecutor();

   ExecutorFactoryConfigurationBuilder asyncTransportExecutor();
   
   ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor();

   ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor();

   ShutdownConfigurationBuilder shutdown();

   GlobalConfiguration build();
}