package org.infinispan.jcache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLoaderConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

public class JCacheLoaderAdapterConfigurationBuilder extends AbstractLoaderConfigurationBuilder<JCacheLoaderAdapterConfiguration, JCacheLoaderAdapterConfigurationBuilder> {

   public JCacheLoaderAdapterConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public void validate() {
   }

   @Override
   public JCacheLoaderAdapterConfiguration create() {
      return new JCacheLoaderAdapterConfiguration(TypedProperties.toTypedProperties(properties));
   }

   @Override
   public Builder<?> read(JCacheLoaderAdapterConfiguration template) {
      return this;
   }

   @Override
   public JCacheLoaderAdapterConfigurationBuilder self() {
      return this;
   }



}
