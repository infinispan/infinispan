package org.infinispan.configuration.cache;

import org.infinispan.configuration.parsing.XmlConfigHelper;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ClusterLoaderConfigurationBuilder extends AbstractStoreConfigurationBuilder<ClusterLoaderConfiguration, ClusterLoaderConfigurationBuilder> {
   private long remoteCallTimeout;

   public ClusterLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public ClusterLoaderConfigurationBuilder self() {
      return this;
   }

   public ClusterLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout) {
      this.remoteCallTimeout = remoteCallTimeout;
      return this;
   }

   public ClusterLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout, TimeUnit unit) {
      this.remoteCallTimeout = unit.toMillis(remoteCallTimeout);
      return this;
   }

   @Override
   public ClusterLoaderConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterLoaderConfiguration create() {
      return new ClusterLoaderConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                 singletonStore.create(), preload, shared, properties, remoteCallTimeout);
   }

   @Override
   public ClusterLoaderConfigurationBuilder read(ClusterLoaderConfiguration template) {
      super.read(template);
      this.remoteCallTimeout = template.remoteCallTimeout();
      this.properties = template.properties();
      return this;
   }
}
