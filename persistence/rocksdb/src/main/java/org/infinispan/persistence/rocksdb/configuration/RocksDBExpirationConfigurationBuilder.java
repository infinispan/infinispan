package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRY_QUEUE_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * since 10.0
 */
public class RocksDBExpirationConfigurationBuilder implements Builder<RocksDBExpirationConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   RocksDBExpirationConfigurationBuilder() {
      attributes = RocksDBExpirationConfiguration.attributeDefinitionSet();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RocksDBExpirationConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RocksDBExpirationConfigurationBuilder expiredLocation(String expiredLocation) {
      attributes.attribute(EXPIRED_LOCATION).set(expiredLocation);
      return this;
   }


   RocksDBExpirationConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      attributes.attribute(EXPIRY_QUEUE_SIZE).set(expiryQueueSize);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public RocksDBExpirationConfiguration create() {
      return new RocksDBExpirationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(RocksDBExpirationConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
