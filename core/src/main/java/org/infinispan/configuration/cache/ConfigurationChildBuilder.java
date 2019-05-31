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

   IndexingConfigurationBuilder indexing();

   InvocationBatchingConfigurationBuilder invocationBatching();

   JMXStatisticsConfigurationBuilder jmxStatistics();

   PersistenceConfigurationBuilder persistence();

   LockingConfigurationBuilder locking();

   SecurityConfigurationBuilder security();

   TransactionConfigurationBuilder transaction();

   UnsafeConfigurationBuilder unsafe();

   SitesConfigurationBuilder sites();

   /**
    * @deprecated since 9.4, replace with {@link ConfigurationBuilder#encoding()} to specify the key and value's {@link org.infinispan.commons.dataconversion.MediaType} as "application/x-java-object", or preferably, avoid storing unmarshalled content when using the server and use the new <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#embedded_remote_interoperability_a_id_endpoint_interop_a">endpoint interoperability mechanism</a>
    */
   @Deprecated
   CompatibilityModeConfigurationBuilder compatibility();

   MemoryConfigurationBuilder memory();

   default ConfigurationChildBuilder template(boolean template) {
      return this;
   }

   void validate(GlobalConfiguration globalConfig);

   Configuration build();
}
