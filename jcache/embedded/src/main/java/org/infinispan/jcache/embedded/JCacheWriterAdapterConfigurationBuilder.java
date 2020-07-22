package org.infinispan.jcache.embedded;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JCacheWriterAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JCacheWriterAdapterConfiguration, JCacheWriterAdapterConfigurationBuilder> {

   public JCacheWriterAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JCacheWriterAdapterConfiguration.attributeDefinitionSet());

      // JCache doesn't support segmentation
      segmented(false);
   }

   @Override
   public JCacheWriterAdapterConfigurationBuilder segmented(boolean b) {
      if (b) {
         throw new UnsupportedOperationException("JCache does not support being segmented!");
      }
      return super.segmented(b);
   }

   @Override
   public JCacheWriterAdapterConfiguration create() {
      return new JCacheWriterAdapterConfiguration(attributes.protect(), async.create());
   }

   @Override
   public JCacheWriterAdapterConfigurationBuilder self() {
      return this;
   }

}
