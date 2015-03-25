package org.jboss.as.clustering.infinispan.cs.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * StoreConfigurationBuilder used for stores/loaders that don't have a configuration builder
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class DeployedStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<DeployedStoreConfiguration, DeployedStoreConfigurationBuilder> {

   private PersistenceConfigurationBuilder persistenceConfigurationBuilder;
   private String customStoreClassName;

   public DeployedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      this.persistenceConfigurationBuilder = builder;
   }

   @Override
   public DeployedStoreConfiguration create() {
      return new DeployedStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,
              async.create(), singletonStore.create(), preload, shared, properties, persistenceConfigurationBuilder, customStoreClassName);
   }

   @Override
   public Builder<?> read(DeployedStoreConfiguration template) {
      super.read(template);
      this.persistenceConfigurationBuilder = template.getPersistenceConfigurationBuilder();
      this.customStoreClassName = template.getCustomStoreClassName();
      return this;
   }

   @Override
   public DeployedStoreConfigurationBuilder self() {
      return this;
   }

   public DeployedStoreConfigurationBuilder customStoreClassName(String customStoreClassName) {
      this.customStoreClassName = customStoreClassName;
      return this;
   }
}
