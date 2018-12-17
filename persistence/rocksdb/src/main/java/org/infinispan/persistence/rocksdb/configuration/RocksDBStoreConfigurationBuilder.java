package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.BLOCK_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CACHE_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CLEAR_THRESHOLD;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.COMPRESSION_TYPE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.EXPIRY_QUEUE_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.LOCATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class RocksDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RocksDBStoreConfiguration, RocksDBStoreConfigurationBuilder>
      implements ConfigurationBuilderInfo {

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RocksDBStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RocksDBStoreConfiguration.ELEMENT_DEFINTION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
