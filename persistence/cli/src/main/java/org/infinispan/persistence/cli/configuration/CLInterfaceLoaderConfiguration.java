package org.infinispan.persistence.cli.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ClusterLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.cli.CLInterfaceLoader;

import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@BuiltBy(CLInterfaceLoaderConfigurationBuilder.class)
@ConfigurationFor(CLInterfaceLoader.class)
public class CLInterfaceLoaderConfiguration extends AbstractStoreConfiguration {

   private final String connectionString;

   public CLInterfaceLoaderConfiguration(
         boolean purgeOnStartup, boolean fetchPersistentState,
         boolean ignoreModifications, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore, boolean preload,
         boolean shared, Properties properties,
         String connectionString) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async,
            singletonStore, preload, shared, properties);
      this.connectionString = connectionString;
   }

   public String connectionString() {
      return connectionString;
   }

   @Override
   public String toString() {
      return "CLInterfaceLoaderConfiguration{" +
            "connectionString='" + connectionString + '\'' +
            '}';
   }

}
