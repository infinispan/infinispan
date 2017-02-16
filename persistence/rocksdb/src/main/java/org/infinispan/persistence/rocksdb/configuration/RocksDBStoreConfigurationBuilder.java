package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.BLOCK_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CACHE_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CLEAR_THRESHOLD;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.EXPIRY_QUEUE_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.COMPRESSION_TYPE;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class RocksDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RocksDBStoreConfiguration, RocksDBStoreConfigurationBuilder> {

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RocksDBStoreConfiguration.attributeDefinitionSet());
   }

   public RocksDBStoreConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
      return self();
   }

   public RocksDBStoreConfigurationBuilder expiredLocation(String expiredLocation) {
      attributes.attribute(EXPIRED_LOCATION).set(expiredLocation);
      return self();
   }

   public RocksDBStoreConfigurationBuilder blockSize(int blockSize) {
      attributes.attribute(BLOCK_SIZE).set(blockSize);
      return self();
   }

   public RocksDBStoreConfigurationBuilder cacheSize(long cacheSize) {
      attributes.attribute(CACHE_SIZE).set(cacheSize);
      return self();
   }

   public RocksDBStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      attributes.attribute(EXPIRY_QUEUE_SIZE).set(expiryQueueSize);
      return self();
   }

   public RocksDBStoreConfigurationBuilder clearThreshold(int clearThreshold) {
      attributes.attribute(CLEAR_THRESHOLD).set(clearThreshold);
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
   }

   @Override
   public RocksDBStoreConfiguration create() {
      return new RocksDBStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(RocksDBStoreConfiguration template) {
      super.read(template);
      return self();
   }

   @Override
   public RocksDBStoreConfigurationBuilder self() {
      return this;
   }

}
