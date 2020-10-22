package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBExpirationConfiguration.EXPIRED_LOCATION;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.BLOCK_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CACHE_SIZE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.CLEAR_THRESHOLD;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.COMPRESSION_TYPE;
import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration.LOCATION;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.PersistenceUtil;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class RocksDBStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RocksDBStoreConfiguration, RocksDBStoreConfigurationBuilder>
      implements ConfigurationBuilderInfo {

   protected RocksDBExpirationConfigurationBuilder expiration = new RocksDBExpirationConfigurationBuilder();

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      this(builder, RocksDBStoreConfiguration.attributeDefinitionSet());
   }

   public RocksDBStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributeSet) {
      super(builder, attributeSet);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RocksDBStoreConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return Collections.singletonList(expiration);
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
      expiration.expiredLocation(expiredLocation);
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

   /**
    * @deprecated Since 10.1, there is no more queue in {@link org.infinispan.persistence.rocksdb.RocksDBStore}
    */
   @Deprecated
   public RocksDBStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      expiration.expiryQueueSize(expiryQueueSize);
      return self();
   }

   /**
    * @deprecated Since 12.0, no longer used. Will be removed in 15.0
    */
   @Deprecated
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
   public Builder<?> read(RocksDBStoreConfiguration template) {
      super.read(template);
      expiration.read(template.expiration());
      return self();
   }

   @Override
   public RocksDBStoreConfigurationBuilder self() {
      return this;
   }

}
