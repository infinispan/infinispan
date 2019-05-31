package org.infinispan.jcache.embedded;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JCacheWriterAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JCacheWriterAdapterConfiguration, JCacheWriterAdapterConfigurationBuilder> {

   public JCacheWriterAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JCacheWriterAdapterConfiguration.attributeDefinitionSet());
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
