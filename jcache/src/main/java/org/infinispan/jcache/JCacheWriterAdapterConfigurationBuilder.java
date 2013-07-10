package org.infinispan.jcache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

public class JCacheWriterAdapterConfigurationBuilder extends AbstractStoreConfigurationBuilder<JCacheWriterAdapterConfiguration, JCacheWriterAdapterConfigurationBuilder> {

   public JCacheWriterAdapterConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public JCacheWriterAdapterConfiguration create() {
      return new JCacheWriterAdapterConfiguration(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
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
