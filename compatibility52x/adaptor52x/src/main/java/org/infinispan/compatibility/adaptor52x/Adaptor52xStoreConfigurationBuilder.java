package org.infinispan.compatibility.adaptor52x;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.CacheLoader;

import java.util.Properties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Adaptor52xStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<Adaptor52xStoreConfiguration, Adaptor52xStoreConfigurationBuilder> {


   private CacheLoader loader;

   public Adaptor52xStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public Adaptor52xStoreConfiguration create() {
      return new Adaptor52xStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                              singletonStore.create(), preload, shared, properties, loader);
   }

   @Override
   public Builder<?> read(Adaptor52xStoreConfiguration template) {
      this.loader = template.getLoader();
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      async.read(template.async());
      singletonStore.read(template.singletonStore());
      preload = template.preload();
      shared = template.shared();
      return this;
   }

   @Override
   public Adaptor52xStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public Adaptor52xStoreConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return self();
   }

   public CacheLoader getLoader() {
      return loader;
   }

   public Adaptor52xStoreConfigurationBuilder loader(CacheLoader loader) {
      this.loader = loader;
      return self();
   }
}
