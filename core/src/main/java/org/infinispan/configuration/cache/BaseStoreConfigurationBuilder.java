package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

/**
 * StoreConfigurationBuilder used for stores/loaders that don't have a configuration builder
 *
 * @author wburns
 * @since 7.0
 */
public class BaseStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<AbstractStoreConfiguration, BaseStoreConfigurationBuilder> {
   public BaseStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AbstractStoreConfiguration create() {
      return new AbstractStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                            singletonStore.create(), preload, shared, properties);
   }

   @Override
   public BaseStoreConfigurationBuilder self() {
      return this;
   }
}
