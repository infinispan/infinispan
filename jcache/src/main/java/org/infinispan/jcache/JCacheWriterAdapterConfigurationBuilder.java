package org.infinispan.jcache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class JCacheWriterAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JCacheWriterAdapterConfiguration, JCacheWriterAdapterConfigurationBuilder> {

   public JCacheWriterAdapterConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public JCacheWriterAdapterConfiguration create() {
      return new JCacheWriterAdapterConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,async.create(), singletonStore.create(), preload, shared, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public Builder<?> read(JCacheWriterAdapterConfiguration template) {
      return this;
   }

   @Override
   public JCacheWriterAdapterConfigurationBuilder self() {
      return this;
   }

}
