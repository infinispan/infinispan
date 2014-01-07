package org.infinispan.configuration.global;

abstract class AbstractGlobalConfigurationBuilder implements GlobalConfigurationChildBuilder {

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

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #listenerExecutorName(String)} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return globalConfig.asyncListenerExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #persistenceExecutorName(String)} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder persistenceExecutor() {
      return globalConfig.persistenceExecutor();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link TransportConfigurationBuilder#remoteCommandExecutorName(String)} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder remoteCommandsExecutor() {
      return globalConfig.remoteCommandsExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #evictionExecutorName(String)} instead.
    */
   @Deprecated
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #replicationQueueExecutorName(String)} instead.
    */
   @Deprecated
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   @Override
   public GlobalSecurityConfigurationBuilder security() {
      return globalConfig.security();
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

}