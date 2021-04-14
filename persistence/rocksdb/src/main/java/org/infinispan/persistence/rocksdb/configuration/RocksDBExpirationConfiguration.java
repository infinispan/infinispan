package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class RocksDBExpirationConfiguration {

   final static AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.PATH, null, String.class).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> EXPIRY_QUEUE_SIZE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.EXPIRY_QUEUE_SIZE, 10000).immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBExpirationConfiguration.class, EXPIRED_LOCATION, EXPIRY_QUEUE_SIZE);
   }

   private final Attribute<String> expiredLocation;
   private final Attribute<Integer> expiryQueueSize;

   RocksDBExpirationConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      expiredLocation = attributes.attribute(EXPIRED_LOCATION);
      expiryQueueSize = attributes.attribute(EXPIRY_QUEUE_SIZE);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String expiredLocation() {
      return expiredLocation.get();
   }

   /**
    * @deprecated Since 10.1, there is no more queue in {@link org.infinispan.persistence.rocksdb.RocksDBStore}
    */
   @Deprecated
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
