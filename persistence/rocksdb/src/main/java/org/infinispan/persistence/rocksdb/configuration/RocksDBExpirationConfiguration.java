package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
public class RocksDBExpirationConfiguration extends ConfigurationElement<RocksDBExpirationConfiguration> {

   final static AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.PATH, null, String.class).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> EXPIRY_QUEUE_SIZE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.EXPIRY_QUEUE_SIZE, 10000).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBExpirationConfiguration.class, EXPIRED_LOCATION, EXPIRY_QUEUE_SIZE);
   }

   RocksDBExpirationConfiguration(AttributeSet attributes) {
      super(Element.EXPIRATION, attributes);
   }


   public String expiredLocation() {
      return attributes.attribute(EXPIRED_LOCATION).get();
   }

   /**
    * @deprecated Since 10.1, there is no more queue in {@link org.infinispan.persistence.rocksdb.RocksDBStore}
    */
   @Deprecated
   int expiryQueueSize() {
      return attributes.attribute(EXPIRY_QUEUE_SIZE).get();
   }
}
