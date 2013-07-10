package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.loaders.cluster.ClusterCacheLoader;

/**
 * ClusterCacheLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterCacheLoaderConfigurationBuilder.class)
@ConfigurationFor(ClusterCacheLoader.class)
public class ClusterCacheLoaderConfiguration extends AbstractLoaderConfiguration {
   private final long remoteCallTimeout;

   ClusterCacheLoaderConfiguration(long remoteCallTimeout, TypedProperties properties) {
      super(properties);
      this.remoteCallTimeout = remoteCallTimeout;
   }

   public long remoteCallTimeout() {
      return remoteCallTimeout;
   }

   @Override
   public String toString() {
      return "ClusterCacheLoaderConfiguration [remoteCallTimeout=" + remoteCallTimeout + "]";
   }

}
