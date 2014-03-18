package org.infinispan.nearcache.jms;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class RemoteEventStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteEventStoreConfiguration, RemoteEventStoreConfigurationBuilder> {


   public RemoteEventStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public RemoteEventStoreConfiguration create() {
      return new RemoteEventStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,
                                                    async.create(), singletonStore.create(),preload, shared, properties);
   }

   @Override
   public RemoteEventStoreConfigurationBuilder self() {
      return this;
   }
}
