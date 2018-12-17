package org.infinispan.persistence.jpa.configuration;

import static org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration.ENTITY_CLASS;
import static org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration.PERSISTENCE_UNIT_NAME;
import static org.infinispan.persistence.jpa.configuration.JpaStoreConfiguration.STORE_METADATA;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<JpaStoreConfiguration, JpaStoreConfigurationBuilder> implements ConfigurationBuilderInfo {

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

   @Override
   public ElementDefinition getElementDefinition() {
      return JpaStoreConfiguration.ELEMENT_DEFINITION;
   }


   @Deprecated
   public JpaStoreConfigurationBuilder batchSize(long batchSize) {
      int size = batchSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (batchSize < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) batchSize);
      maxBatchSize(size);
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
