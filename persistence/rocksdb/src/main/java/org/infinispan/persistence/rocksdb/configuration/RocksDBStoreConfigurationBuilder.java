package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.COMPRESSION_TYPE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.LOCATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.PersistenceUtil;

/**
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 */
public class RocksDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RocksDBStoreConfiguration, RocksDBStoreConfigurationBuilder> {

   protected RocksDBExpirationConfigurationBuilder expiration = new RocksDBExpirationConfigurationBuilder();

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      this(builder, RocksDBStoreConfiguration.attributeDefinitionSet());
   }

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributeSet) {
      super(builder, attributeSet);
   }

   public RocksDBStoreConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
      return self();
   }

   public RocksDBStoreConfigurationBuilder expiredLocation(String expiredLocation) {
      expiration.expiredLocation(expiredLocation);
      return self();
   }

   public RocksDBStoreConfigurationBuilder compressionType(CompressionType compressionType) {
      attributes.attribute(COMPRESSION_TYPE).set(compressionType);
      return self();
   }

   @Override
   public void validate() {
      // how do you validate required attributes?
      super.validate();
      expiration.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      PersistenceUtil.validateGlobalStateStoreLocation(globalConfig, RocksDBStoreConfiguration.class.getSimpleName(),
            attributes.attribute(LOCATION),
            expiration.attributes().attribute(EXPIRED_LOCATION));

      super.validate(globalConfig);
   }

   @Override
   public RocksDBStoreConfiguration create() {
      return new RocksDBStoreConfiguration(attributes.protect(), async.create(), expiration.create());
   }

   @Override
   public Builder<?> read(RocksDBStoreConfiguration template, Combine combine) {
      super.read(template, combine);
      expiration.read(template.expiration(), combine);
      return self();
   }

   @Override
   public RocksDBStoreConfigurationBuilder self() {
      return this;
   }

}
