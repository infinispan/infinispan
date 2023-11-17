package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRED_LOCATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
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

   @Override
   public RocksDBExpirationConfiguration create() {
      return new RocksDBExpirationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(RocksDBExpirationConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

}
