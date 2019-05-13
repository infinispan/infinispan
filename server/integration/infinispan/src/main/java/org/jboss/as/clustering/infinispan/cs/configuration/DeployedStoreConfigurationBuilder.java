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

   public DeployedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, DeployedStoreConfiguration.attributeDefinitionSet());
      this.persistenceConfigurationBuilder = builder;
   }

   @Override
   public DeployedStoreConfiguration create() {
      return new DeployedStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(), persistenceConfigurationBuilder);
   }

   @Override
   public Builder<?> read(DeployedStoreConfiguration template) {
      super.read(template);
      this.persistenceConfigurationBuilder = template.getPersistenceConfigurationBuilder();
      return this;
   }

   @Override
   public DeployedStoreConfigurationBuilder self() {
      return this;
   }

   public DeployedStoreConfigurationBuilder name(String name) {
      attributes.attribute(DeployedStoreConfiguration.NAME).set(name);
      return this;
   }

   public DeployedStoreConfigurationBuilder customStoreClassName(String customStoreClassName) {
      attributes.attribute(DeployedStoreConfiguration.CUSTOM_STORE_CLASS_NAME).set(customStoreClassName);
      return this;
   }

   @Override
   public void validate() {
      super.validate(true);
   }
}
