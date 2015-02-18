package org.infinispan.persistence.jpa.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jpa.JpaStore;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@BuiltBy(JpaStoreConfigurationBuilder.class)
@ConfigurationFor(JpaStore.class)
public class JpaStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<String> PERSISTENCE_UNIT_NAME = AttributeDefinition.builder("persistenceUnitName", null, String.class).immutable().build();
   static final AttributeDefinition<Class> ENTITY_CLASS = AttributeDefinition.builder("entityClass", null, Class.class).immutable().build();
   static final AttributeDefinition<Long> BATCH_SIZE = AttributeDefinition.builder("batchSize", 100l).immutable().build();
   static final AttributeDefinition<Boolean> STORE_METADATA = AttributeDefinition.builder("storeMetadata", true).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JpaStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), PERSISTENCE_UNIT_NAME, ENTITY_CLASS, BATCH_SIZE, STORE_METADATA);
   }

   private final Attribute<String> persistenceUnitName;
   private final Attribute<Class> entityClass;
   private final Attribute<Long> batchSize;
   private final Attribute<Boolean> storeMetadata;

   protected JpaStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      persistenceUnitName = attributes.attribute(PERSISTENCE_UNIT_NAME);
      entityClass = attributes.attribute(ENTITY_CLASS);
      batchSize = attributes.attribute(BATCH_SIZE);
      storeMetadata = attributes.attribute(STORE_METADATA);
   }

   public String persistenceUnitName() {
      return persistenceUnitName.get();
   }

   public Class<?> entityClass() {
      return entityClass.get();
   }

   public long batchSize() {
      return batchSize.get();
   }

   public boolean storeMetadata() {
      return storeMetadata.get();
   }
}
