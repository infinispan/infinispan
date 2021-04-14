package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.rocksdb.RocksDBStore;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@ConfigurationFor(RocksDBStore.class)
@BuiltBy(RocksDBStoreConfigurationBuilder.class)
@SerializedWith(RocksDBStoreConfigurationSerializer.class)
public class RocksDBStoreConfiguration extends AbstractStoreConfiguration {

   final static AttributeDefinition<String> LOCATION = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.PATH, null, String.class).immutable().build();
   public final static AttributeDefinition<CompressionType> COMPRESSION_TYPE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.COMPRESSION_TYPE, CompressionType.NONE).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> BLOCK_SIZE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.BLOCK_SIZE, 0).immutable().build();
   final static AttributeDefinition<Long> CACHE_SIZE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.CACHE_SIZE, 0l).immutable().build();
   final static AttributeDefinition<Integer> CLEAR_THRESHOLD = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.CLEAR_THRESHOLD, 10000).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, COMPRESSION_TYPE,
            BLOCK_SIZE, CACHE_SIZE, CLEAR_THRESHOLD);
   }

   private final Attribute<String> location;
   private final Attribute<CompressionType> compressionType;
   private final Attribute<Integer> blockSize;
   private final Attribute<Long> cacheSize;
   private final Attribute<Integer> clearThreshold;
   private final RocksDBExpirationConfiguration expiration;

   public RocksDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, RocksDBExpirationConfiguration expiration) {
      super(attributes, async);
      location = attributes.attribute(LOCATION);
      compressionType = attributes.attribute(COMPRESSION_TYPE);
      blockSize = attributes.attribute(BLOCK_SIZE);
      cacheSize = attributes.attribute(CACHE_SIZE);
      clearThreshold = attributes.attribute(CLEAR_THRESHOLD);
      this.expiration = expiration;
   }

   public RocksDBExpirationConfiguration expiration() {
      return expiration;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String location() {
      return location.get();
   }

   public String expiredLocation() {
      return expiration.expiredLocation();
   }

   public CompressionType compressionType() {
      return compressionType.get();
   }

   public Integer blockSize() {
      return blockSize.get();
   }

   public Long cacheSize() {
      return cacheSize.get();
   }

   /**
    * @deprecated There is no more queue in {@link org.infinispan.persistence.rocksdb.RocksDBStore}
    */
   @Deprecated
   public int expiryQueueSize() {
      return expiration.expiryQueueSize();
   }

   /**
    * @deprecated Since 12.0, no longer used. Will be removed in 15.0
    * @return the configured clear threshold
    */
   @Deprecated
   public int clearThreshold() {
      return clearThreshold.get();
   }
}
