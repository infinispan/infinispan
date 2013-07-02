package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;

/**
 * ClusterCacheLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterCacheLoaderConfigurationBuilder.class)
public class ClusterCacheLoaderConfiguration extends AbstractLoaderConfiguration implements LegacyLoaderAdapter<ClusterCacheLoaderConfig> {
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

   @Override
   public ClusterCacheLoaderConfig adapt() {
      ClusterCacheLoaderConfig config = new ClusterCacheLoaderConfig();
      config.remoteCallTimeout(remoteCallTimeout);
      XmlConfigHelper.setValues(config, properties(), false, true);
      return config;
   }


}
