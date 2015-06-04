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
    * Set thread pool via {@link #listenerThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return globalConfig.asyncListenerExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #persistenceThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder persistenceExecutor() {
      return globalConfig.persistenceExecutor();
   }

   /**
    * @deprecated This method always returns {@code null} now.
    * Set thread pool via {@link TransportConfigurationBuilder#transportThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link TransportConfigurationBuilder#remoteCommandThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ExecutorFactoryConfigurationBuilder remoteCommandsExecutor() {
      return globalConfig.remoteCommandsExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #evictionThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   /**
    * @deprecated This method always returns null now.
    * Set thread pool via {@link #replicationQueueThreadPool()} instead.
    */
   @Deprecated
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   @Override
   public ThreadPoolConfigurationBuilder listenerThreadPool() {
      return globalConfig.listenerThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return globalConfig.asyncThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder replicationQueueThreadPool() {
      return globalConfig.replicationQueueThreadPool();
   }

   @Deprecated
   @Override
   public ThreadPoolConfigurationBuilder evictionThreadPool() {
      return globalConfig.expirationThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder expirationThreadPool() {
      return globalConfig.expirationThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder persistenceThreadPool() {
      return globalConfig.persistenceThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder stateTransferThreadPool() {
      return globalConfig.stateTransferThreadPool();
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