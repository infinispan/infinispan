package org.infinispan.configuration.global;

abstract class AbstractGlobalConfigurationBuilder<T> implements GlobalConfigurationChildBuilder {

   private final GlobalConfigurationBuilder globalConfig;

   protected AbstractGlobalConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      this.globalConfig = globalConfig;
   }

   protected GlobalConfigurationBuilder getGlobalConfig() {
      return globalConfig;
   }

   @Override
   public TransportConfigurationBuilder transport() {
      return globalConfig.transport();
   }

   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      globalConfig.globalJmxStatistics().enable();
      return globalConfig.globalJmxStatistics();
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return globalConfig.serialization();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return globalConfig.asyncListenerExecutor();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder persistenceExecutor() {
      return globalConfig.persistenceExecutor();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder remoteCommandsExecutor() {
      return globalConfig.remoteCommandsExecutor();
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return globalConfig.shutdown();
   }

   @Override
   public SiteConfigurationBuilder site() {
      return globalConfig.site();
   }

   @Override
   public GlobalConfiguration build() {
      return globalConfig.build();
   }

   abstract void validate();

   abstract T create();

   protected abstract GlobalConfigurationChildBuilder read(T template);

}