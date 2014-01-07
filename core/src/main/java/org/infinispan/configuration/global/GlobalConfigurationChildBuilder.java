package org.infinispan.configuration.global;

public interface GlobalConfigurationChildBuilder {
   TransportConfigurationBuilder transport();

   GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics();

   SerializationConfigurationBuilder serialization();

   ExecutorFactoryConfigurationBuilder asyncListenerExecutor();

   ExecutorFactoryConfigurationBuilder persistenceExecutor();

   ExecutorFactoryConfigurationBuilder asyncTransportExecutor();

   ExecutorFactoryConfigurationBuilder remoteCommandsExecutor();

   ThreadPoolConfigurationBuilder listenerThreadPool();

   ThreadPoolConfigurationBuilder replicationQueueThreadPool();

   ThreadPoolConfigurationBuilder evictionThreadPool();

   ThreadPoolConfigurationBuilder persistenceThreadPool();

   ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor();

   ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor();

   GlobalSecurityConfigurationBuilder security();

   ShutdownConfigurationBuilder shutdown();

   SiteConfigurationBuilder site();

   GlobalConfiguration build();
}