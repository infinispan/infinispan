package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
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
   final static AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", "Infinispan-RocksDBStore/data").immutable().xmlName("path").build();
   final static AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder("expiredLocation", "Infinispan-RocksDBStore/expired").immutable().autoPersist(false).xmlName("path").build();
   final static AttributeDefinition<CompressionType> COMPRESSION_TYPE = AttributeDefinition.builder("compressionType", CompressionType.NONE).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> BLOCK_SIZE = AttributeDefinition.builder("blockSize", 0).immutable().build();
   final static AttributeDefinition<Long> CACHE_SIZE = AttributeDefinition.builder("cacheSize", 0l).immutable().build();
   final static AttributeDefinition<Integer> EXPIRY_QUEUE_SIZE = AttributeDefinition.builder("expiryQueueSize", 10000).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> CLEAR_THRESHOLD = AttributeDefinition.builder("clearThreshold", 10000).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, EXPIRED_LOCATION, COMPRESSION_TYPE,
            BLOCK_SIZE, CACHE_SIZE, EXPIRY_QUEUE_SIZE, CLEAR_THRESHOLD);
   }

   private final Attribute<String> location;
   private final Attribute<String> expiredLocation;
   private final Attribute<CompressionType> compressionType;
   private final Attribute<Integer> blockSize;
   private final Attribute<Long> cacheSize;
   private final Attribute<Integer> expiryQueueSize;
   private final Attribute<Integer> clearThreshold;

   public RocksDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      location = attributes.attribute(LOCATION);
      expiredLocation = attributes.attribute(EXPIRED_LOCATION);
      compressionType = attributes.attribute(COMPRESSION_TYPE);
      blockSize = attributes.attribute(BLOCK_SIZE);
      cacheSize = attributes.attribute(CACHE_SIZE);
      expiryQueueSize = attributes.attribute(EXPIRY_QUEUE_SIZE);
      clearThreshold = attributes.attribute(CLEAR_THRESHOLD);
   }

   public String location() {
      return location.get();
   }

   public String expiredLocation() {
      return expiredLocation.get();
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

   public int expiryQueueSize() {
      return expiryQueueSize.get();
   }

   public int clearThreshold() {
      return clearThreshold.get();
   }
}
