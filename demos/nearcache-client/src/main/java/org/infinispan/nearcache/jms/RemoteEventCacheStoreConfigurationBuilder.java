package org.infinispan.nearcache.jms;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

public class RemoteEventCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteEventCacheStoreConfiguration, RemoteEventCacheStoreConfigurationBuilder> {

   public RemoteEventCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public RemoteEventCacheStoreConfiguration create() {
      return new RemoteEventCacheStoreConfiguration(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(RemoteEventCacheStoreConfiguration template) {
      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      return this;
   }

   @Override
   public RemoteEventCacheStoreConfigurationBuilder self() {
      return this;
   }

}
