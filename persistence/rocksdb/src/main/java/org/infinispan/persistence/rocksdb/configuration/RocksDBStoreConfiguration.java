package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.Element.ROCKSDB_STORE;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
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
public class RocksDBStoreConfiguration extends AbstractStoreConfiguration implements ConfigurationInfo {

   final static AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", null, String.class).immutable().xmlName("path").build();
   public final static AttributeDefinition<CompressionType> COMPRESSION_TYPE = AttributeDefinition.builder("compressionType", CompressionType.NONE).immutable().autoPersist(false).build();
   final static AttributeDefinition<Integer> BLOCK_SIZE = AttributeDefinition.builder("blockSize", 0).immutable().build();
   final static AttributeDefinition<Long> CACHE_SIZE = AttributeDefinition.builder("cacheSize", 0l).immutable().build();
   final static AttributeDefinition<Integer> CLEAR_THRESHOLD = AttributeDefinition.builder("clearThreshold", 10000).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, COMPRESSION_TYPE,
            BLOCK_SIZE, CACHE_SIZE, CLEAR_THRESHOLD);
   }

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(ROCKSDB_STORE.getLocalName(), true, false);

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

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return Collections.singletonList(expiration);
   }

   @Override
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
    * @deprecated clearThreshold is no longer being used.
    * @return the configured clear threshold
    */
   @Deprecated
   public int clearThreshold() {
      return clearThreshold.get();
   }
}
