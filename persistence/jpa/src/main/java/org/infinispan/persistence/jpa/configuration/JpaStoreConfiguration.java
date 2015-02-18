package org.infinispan.persistence.jpa.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
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

   public static AttributeSet attributeSet() {
      return new AttributeSet(JpaStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), PERSISTENCE_UNIT_NAME, ENTITY_CLASS, BATCH_SIZE, STORE_METADATA);
   }

   protected JpaStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public String persistenceUnitName() {
      return attributes.attribute(PERSISTENCE_UNIT_NAME).asString();
   }

   public Class<?> entityClass() {
      return attributes.attribute(ENTITY_CLASS).asObject(Class.class);
   }

   public long batchSize() {
      return attributes.attribute(BATCH_SIZE).asLong();
   }

   public boolean storeMetadata() {
      return attributes.attribute(STORE_METADATA).asBoolean();
   }
}
