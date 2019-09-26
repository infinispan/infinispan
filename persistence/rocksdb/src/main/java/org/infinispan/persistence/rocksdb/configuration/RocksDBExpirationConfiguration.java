package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.Element.EXPIRATION;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class RocksDBExpirationConfiguration implements ConfigurationInfo {

   final static AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder("path", null, String.class).immutable().autoPersist(false).xmlName("path").build();
   final static AttributeDefinition<Integer> EXPIRY_QUEUE_SIZE = AttributeDefinition.builder("queueSize", 10000).immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBExpirationConfiguration.class, EXPIRED_LOCATION, EXPIRY_QUEUE_SIZE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(EXPIRATION.getLocalName());

   private final Attribute<String> expiredLocation;
   private final Attribute<Integer> expiryQueueSize;

   RocksDBExpirationConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      expiredLocation = attributes.attribute(EXPIRED_LOCATION);
      expiryQueueSize = attributes.attribute(EXPIRY_QUEUE_SIZE);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String expiredLocation() {
      return expiredLocation.get();
   }

   int expiryQueueSize() {
      return expiryQueueSize.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RocksDBExpirationConfiguration that = (RocksDBExpirationConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "RocksDBExpirationConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
