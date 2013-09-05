package org.infinispan.nearcache.jms;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class RemoteEventCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteEventCacheStoreConfiguration, RemoteEventCacheStoreConfigurationBuilder> {


   public RemoteEventCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public RemoteEventCacheStoreConfiguration create() {
      return new RemoteEventCacheStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,
                                                    async.create(), singletonStore.create(),preload, shared, properties);
   }

   @Override
   public Builder<?> read(RemoteEventCacheStoreConfiguration template) {
      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      shared = template.shared();
      preload = template.preload();
      return this;
   }

   @Override
   public RemoteEventCacheStoreConfigurationBuilder self() {
      return this;
   }
}
