package org.infinispan.configuration.cache;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.config.parsing.XmlConfigHelper;

public class ClusterCacheLoaderConfigurationBuilder extends AbstractLoaderConfigurationBuilder<ClusterCacheLoaderConfiguration, ClusterCacheLoaderConfigurationBuilder> {
   private long remoteCallTimeout;

   public ClusterCacheLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder self() {
      return this;
   }

   public ClusterCacheLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout) {
      this.remoteCallTimeout = remoteCallTimeout;
      return this;
   }

   public ClusterCacheLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout, TimeUnit unit) {
      this.remoteCallTimeout = unit.toMillis(remoteCallTimeout);
      return this;
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      // TODO: Remove this and any sign of properties when switching to new cache store configs
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterCacheLoaderConfiguration create() {
      return new ClusterCacheLoaderConfiguration(remoteCallTimeout, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder read(ClusterCacheLoaderConfiguration template) {
      this.remoteCallTimeout = template.remoteCallTimeout();
      this.properties = template.properties();
      return this;
   }
}
