package org.infinispan.jcache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JStoreAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JStoreAdapterConfiguration, JStoreAdapterConfigurationBuilder> {

   public JStoreAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public void validate() {
   }

   @Override
   public JStoreAdapterConfiguration create() {
      return new JStoreAdapterConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                            singletonStore.create(), preload, shared, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public JStoreAdapterConfigurationBuilder self() {
      return this;
   }

}
