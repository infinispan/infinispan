package org.infinispan.persistence.jpa.configuration;

import static org.infinispan.persistence.jpa.configuration.Element.JPA_STORE;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.jpa.JpaStore;

/**
 * JpaStoreConfiguration.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 6.0
 */
@BuiltBy(JpaStoreConfigurationBuilder.class)
@ConfigurationFor(JpaStore.class)
@SerializedWith(JpaStoreConfigurationSerializer.class)
public class JpaStoreConfiguration extends AbstractStoreConfiguration {

   static final AttributeDefinition<String> PERSISTENCE_UNIT_NAME = AttributeDefinition.builder("persistenceUnitName", null, String.class).immutable().xmlName("persistence-unit").build();
   static final AttributeDefinition<Class> ENTITY_CLASS = AttributeDefinition.builder("entityClass", null, Class.class).immutable().build();
   static final AttributeDefinition<Boolean> STORE_METADATA = AttributeDefinition.builder("storeMetadata", true).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JpaStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), PERSISTENCE_UNIT_NAME, ENTITY_CLASS, STORE_METADATA);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(JPA_STORE.getLocalName());

   private final Attribute<String> persistenceUnitName;
   private final Attribute<Class> entityClass;
   private final Attribute<Boolean> storeMetadata;

   protected JpaStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      persistenceUnitName = attributes.attribute(PERSISTENCE_UNIT_NAME);
      entityClass = attributes.attribute(ENTITY_CLASS);
      storeMetadata = attributes.attribute(STORE_METADATA);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String persistenceUnitName() {
      return persistenceUnitName.get();
   }

   public Class<?> entityClass() {
      return entityClass.get();
   }

   public long batchSize() {
      return attributes.attribute(MAX_BATCH_SIZE).get();
   }

   public boolean storeMetadata() {
      return storeMetadata.get();
   }
}
