package org.infinispan.configuration.global;

public interface GlobalConfigurationChildBuilder {
   public TransportConfigurationBuilder transport();

   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   public SerializationConfigurationBuilder serialization();

   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor();

   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor();
   
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor();

   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor();

   public ShutdownConfigurationBuilder shutdown();

   public GlobalConfiguration build();
}