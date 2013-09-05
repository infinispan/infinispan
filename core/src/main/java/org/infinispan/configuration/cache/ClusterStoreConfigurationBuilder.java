package org.infinispan.configuration.cache;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.config.parsing.XmlConfigHelper;

public class ClusterStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<ClusterStoreConfiguration, ClusterStoreConfigurationBuilder> {
   private long remoteCallTimeout;

   public ClusterStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public ClusterStoreConfigurationBuilder self() {
      return this;
   }

   public ClusterStoreConfigurationBuilder remoteCallTimeout(long remoteCallTimeout) {
      this.remoteCallTimeout = remoteCallTimeout;
      return this;
   }

   public ClusterStoreConfigurationBuilder remoteCallTimeout(long remoteCallTimeout, TimeUnit unit) {
      this.remoteCallTimeout = unit.toMillis(remoteCallTimeout);
      return this;
   }

   @Override
   public ClusterStoreConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterStoreConfiguration create() {
      return new ClusterStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                                 singletonStore.create(), preload, shared, properties, remoteCallTimeout);
   }

   @Override
   public ClusterStoreConfigurationBuilder read(ClusterStoreConfiguration template) {
      this.remoteCallTimeout = template.remoteCallTimeout();
      this.properties = template.properties();
      return this;
   }
}
