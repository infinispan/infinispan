package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.persistence.cluster.ClusterLoader;

import java.util.Properties;

/**
 * ClusterStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterStoreConfigurationBuilder.class)
@ConfigurationFor(ClusterLoader.class)
public class ClusterStoreConfiguration extends AbstractStoreConfiguration {
   private final long remoteCallTimeout;

   public ClusterStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState,
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
      return "ClusterStoreConfiguration [remoteCallTimeout=" + remoteCallTimeout + "]";
   }

}
