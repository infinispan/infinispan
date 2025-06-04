package org.infinispan.configuration.global;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.manager.EmbeddedCacheManager;

public interface GlobalConfigurationChildBuilder {
   /**
    * Transport-related (i.e. clustering) configuration
    */
   TransportConfigurationBuilder transport();

   /**
    * Global metrics configuration.
    */
   GlobalMetricsConfigurationBuilder metrics();

   /**
    * Global tracing configuration.
    */
   GlobalTracingConfigurationBuilder tracing();

   /**
    * Global JMX configuration.
    */
   GlobalJmxConfigurationBuilder jmx();

   /**
    * Global serialization (i.e. marshalling) configuration
    */
   SerializationConfigurationBuilder serialization();

   /**
    * Configuration for the listener thread pool
    */
   ThreadPoolConfigurationBuilder listenerThreadPool();

   /**
    * Configuration for the expiration thread pool
    */
   ThreadPoolConfigurationBuilder expirationThreadPool();

   /**
    * Configuration for the persistence thread pool
    */
   ThreadPoolConfigurationBuilder persistenceThreadPool();

   /**
    * Configuration for the asynchronous operations thread pool
    */
   ThreadPoolConfigurationBuilder asyncThreadPool();

   /**
    * Configuration for the non blocking thread pool
    */
   ThreadPoolConfigurationBuilder nonBlockingThreadPool();

   /**
    * Configuration for the blocking thread pool
    */
   ThreadPoolConfigurationBuilder blockingThreadPool();

   /**
    * Security-related configuration
    */
   GlobalSecurityConfigurationBuilder security();

   /**
    * Shutdown configuration
    */
   ShutdownConfigurationBuilder shutdown();

   /**
    * Global state configuration
    */
   GlobalStateConfigurationBuilder globalState();

   /**
    * Global modules configuration
    */
   List<Builder<?>> modules();

   /**
    * Module configuration
    */
   <T> T module(Class<T> moduleClass);

   /**
    * Clears all modules from the configuration
    */
   GlobalConfigurationBuilder clearModules();

   /**
    * Adds a module to the configuration
    */
   <T extends Builder<?>> T addModule(Class<T> klass);

   /**
    * Sets the name of the cache that acts as the default cache and is returned by
    * {@link EmbeddedCacheManager#getCache()}. Not
    */
   GlobalConfigurationBuilder defaultCacheName(String defaultCacheName);

   /**
    * Builds a {@link GlobalConfiguration} object using the settings applied to this builder
    */
   GlobalConfiguration build();
}
