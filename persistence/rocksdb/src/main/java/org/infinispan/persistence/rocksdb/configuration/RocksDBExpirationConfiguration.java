package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
public class RocksDBExpirationConfiguration extends ConfigurationElement<RocksDBExpirationConfiguration> {

   static final AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.PATH, null, String.class).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBExpirationConfiguration.class, EXPIRED_LOCATION);
   }

   RocksDBExpirationConfiguration(AttributeSet attributes) {
      super(Element.EXPIRATION, attributes);
   }


   public String expiredLocation() {
      return attributes.attribute(EXPIRED_LOCATION).get();
   }
}
