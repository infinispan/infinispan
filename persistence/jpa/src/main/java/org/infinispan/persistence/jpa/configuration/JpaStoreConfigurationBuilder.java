package org.infinispan.persistence.jpa.configuration;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import static org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration.*;
/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<JpaStoreConfiguration, JpaStoreConfigurationBuilder> {

   public JpaStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JpaStoreConfiguration.attributeDefinitionSet());
   }

   public JpaStoreConfigurationBuilder persistenceUnitName(String persistenceUnitName) {
      attributes.attribute(PERSISTENCE_UNIT_NAME).set(persistenceUnitName);
      return self();
   }

   public JpaStoreConfigurationBuilder entityClass(Class<?> entityClass) {
      attributes.attribute(ENTITY_CLASS).set(entityClass);
      return self();
   }

   public JpaStoreConfigurationBuilder batchSize(long batchSize) {
      attributes.attribute(BATCH_SIZE).set(batchSize);
      return self();
   }

   public JpaStoreConfigurationBuilder storeMetadata(boolean storeMetadata) {
      attributes.attribute(STORE_METADATA).set(storeMetadata);
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
   }

   @Override
   public JpaStoreConfiguration create() {
      return new JpaStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public JpaStoreConfigurationBuilder read(JpaStoreConfiguration template) {
      super.read(template);
      return this;
   }

   @Override
   public JpaStoreConfigurationBuilder self() {
      return this;
   }
}
