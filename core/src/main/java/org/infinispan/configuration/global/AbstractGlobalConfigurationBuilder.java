package org.infinispan.configuration.global;

abstract class AbstractGlobalConfigurationBuilder<T> implements GlobalConfigurationChildBuilder {
   
   private final GlobalConfigurationBuilder globalConfig;
   
   protected AbstractGlobalConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      this.globalConfig = globalConfig;
   }
   
   protected GlobalConfigurationBuilder getGlobalConfig() {
      return globalConfig;
   }

   public TransportConfigurationBuilder transport() {
      return globalConfig.transport();
   }

   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      globalConfig.globalJmxStatistics().enable();
      return globalConfig.globalJmxStatistics();
   }

   public SerializationConfigurationBuilder serialization() {
      return globalConfig.serialization();
   }

   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return globalConfig.asyncListenerExecutor();
   }

   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }
   
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   public ShutdownConfigurationBuilder shutdown() {
      return globalConfig.shutdown();
   }

   public GlobalConfiguration build() {
      return globalConfig.build();
   }
   
   abstract void valididate();
   
   abstract T create();
   
}