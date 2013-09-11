package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.persistence.cluster.ClusterLoader;

import java.util.Properties;

/**
 * ClusterLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterLoaderConfigurationBuilder.class)
@ConfigurationFor(ClusterLoader.class)
public class ClusterLoaderConfiguration extends AbstractStoreConfiguration {
   private final long remoteCallTimeout;

   public ClusterLoaderConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
                                     boolean ignoreModifications, AsyncStoreConfiguration async,
                                     SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties,
                                     long remoteCallTimeout) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.remoteCallTimeout = remoteCallTimeout;
   }

   public long remoteCallTimeout() {
      return remoteCallTimeout;
   }

   @Override
   public String toString() {
      return "ClusterLoaderConfiguration [remoteCallTimeout=" + remoteCallTimeout + "]";
   }

}
