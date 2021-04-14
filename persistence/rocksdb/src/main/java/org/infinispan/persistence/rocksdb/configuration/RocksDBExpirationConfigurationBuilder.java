package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRY_QUEUE_SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * since 10.0
 */
public class RocksDBExpirationConfigurationBuilder implements Builder<RocksDBExpirationConfiguration> {

   private final AttributeSet attributes;

   RocksDBExpirationConfigurationBuilder() {
      attributes = RocksDBExpirationConfiguration.attributeDefinitionSet();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public RocksDBExpirationConfigurationBuilder expiredLocation(String expiredLocation) {
      attributes.attribute(EXPIRED_LOCATION).set(expiredLocation);
      return this;
   }

   /**
    * @deprecated Since 10.1, there is no more queue in {@link org.infinispan.persistence.rocksdb.RocksDBStore}
    */
   @Deprecated
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
