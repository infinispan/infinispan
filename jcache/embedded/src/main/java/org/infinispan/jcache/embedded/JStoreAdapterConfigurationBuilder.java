package org.infinispan.jcache.embedded;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JStoreAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JStoreAdapterConfiguration, JStoreAdapterConfigurationBuilder> {

   public JStoreAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JStoreAdapterConfiguration.attributeDefinitionSet());
   }

   @Override
   public void validate() {
   }

   @Override
   public JStoreAdapterConfiguration create() {
      return new JStoreAdapterConfiguration(attributes.protect(), async.create());
   }

   @Override
   public JStoreAdapterConfigurationBuilder self() {
      return this;
   }

}
