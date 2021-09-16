package org.infinispan.jcache.embedded;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JStoreAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JStoreAdapterConfiguration, JStoreAdapterConfigurationBuilder> {

   public JStoreAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JStoreAdapterConfiguration.attributeDefinitionSet());

      // JCache doesn't support segmentation
      segmented(false);
   }

   @Override
   public JStoreAdapterConfigurationBuilder segmented(boolean b) {
      if (b) {
         throw new UnsupportedOperationException("JCache does not support being segmented!");
      }
      return super.segmented(b);
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
