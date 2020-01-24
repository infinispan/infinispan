package org.infinispan.configuration.global;

import java.util.List;

import org.infinispan.commons.configuration.Builder;

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
   public GlobalMetricsConfigurationBuilder metrics() {
      return globalConfig.metrics();
   }

   @Override
   public GlobalJmxConfigurationBuilder jmx() {
      //TODO [anistor] globalConfig.jmx().enabled(true);  ????
      return globalConfig.jmx();
   }

   @Override
   public GlobalStateConfigurationBuilder globalState() {
      globalConfig.globalState().enable();
      return globalConfig.globalState();
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return globalConfig.serialization();
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
   public ThreadPoolConfigurationBuilder blockingThreadPool() {
      return globalConfig.blockingThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder nonBlockingThreadPool() {
      return globalConfig.nonBlockingThreadPool();
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
   public List<Builder<?>> modules() {
      return globalConfig.modules();
   }

   @Override
   public GlobalConfigurationBuilder defaultCacheName(String defaultCacheName) {
      return globalConfig.defaultCacheName(defaultCacheName);
   }

   @Override
   public GlobalConfiguration build() {
      return globalConfig.build();
   }
}
