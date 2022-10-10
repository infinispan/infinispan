package org.infinispan.configuration.cache;

import org.infinispan.configuration.global.GlobalConfiguration;

public interface ConfigurationChildBuilder {

   ConfigurationChildBuilder simpleCache(boolean simpleCache);

   boolean simpleCache();

   ClusteringConfigurationBuilder clustering();

   /**
    * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
    */
   @Deprecated
   CustomInterceptorsConfigurationBuilder customInterceptors();

   EncodingConfigurationBuilder encoding();

   ExpirationConfigurationBuilder expiration();

   QueryConfigurationBuilder query();

   IndexingConfigurationBuilder indexing();

   InvocationBatchingConfigurationBuilder invocationBatching();

   StatisticsConfigurationBuilder statistics();

   /**
    * @deprecated since 10.1.3 use {@link #statistics} instead. This will be removed in next major version.
    */
   @Deprecated
   default JMXStatisticsConfigurationBuilder jmxStatistics() {
      return statistics();
   }

   PersistenceConfigurationBuilder persistence();

   LockingConfigurationBuilder locking();

   SecurityConfigurationBuilder security();

   TransactionConfigurationBuilder transaction();

   UnsafeConfigurationBuilder unsafe();

   SitesConfigurationBuilder sites();

   MemoryConfigurationBuilder memory();

   default ConfigurationChildBuilder template(boolean template) {
      return this;
   }

   default void validate(GlobalConfiguration globalConfig) {}

   Configuration build();
}
