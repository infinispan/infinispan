package org.infinispan.persistence.leveldb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.leveldb.LevelDBStore;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@ConfigurationFor(LevelDBStore.class)
@BuiltBy(LevelDBStoreConfigurationBuilder.class)
public class LevelDBStoreConfiguration extends AbstractStoreConfiguration {
   public enum ImplementationType {
      AUTO, JAVA, JNI
   }

   final static AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", "Infinispan-LevelDBStore/data").immutable().build();
   final static AttributeDefinition<String> EXPIRED_LOCATION = AttributeDefinition.builder("expiredLocation", "Infinispan-LevelDBStore/expired").immutable().build();
   final static AttributeDefinition<ImplementationType> IMPLEMENTATION_TYPE = AttributeDefinition.builder("implementationType", ImplementationType.AUTO).immutable().build();
   final static AttributeDefinition<CompressionType> COMPRESSION_TYPE = AttributeDefinition.builder("compressionType", CompressionType.NONE).immutable().build();
   final static AttributeDefinition<Integer> BLOCK_SIZE = AttributeDefinition.builder("blockSize", 0).immutable().build();
   final static AttributeDefinition<Long> CACHE_SIZE = AttributeDefinition.builder("cacheSize", 0l).immutable().build();
   final static AttributeDefinition<Integer> EXPIRY_QUEUE_SIZE = AttributeDefinition.builder("expiryQueueSize", 10000).immutable().build();
   final static AttributeDefinition<Integer> CLEAR_THRESHOLD = AttributeDefinition.builder("clearThreshold", 10000).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(LevelDBStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), LOCATION, EXPIRED_LOCATION, IMPLEMENTATION_TYPE, COMPRESSION_TYPE,
            BLOCK_SIZE, CACHE_SIZE, EXPIRY_QUEUE_SIZE, CLEAR_THRESHOLD);
   }

   public LevelDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public String location() {
      return attributes.attribute(LOCATION).asString();
   }

   public String expiredLocation() {
      return attributes.attribute(EXPIRED_LOCATION).asString();
   }

   public ImplementationType implementationType() {
      return attributes.attribute(IMPLEMENTATION_TYPE).asObject(ImplementationType.class);
   }

   public CompressionType compressionType() {
      return attributes.attribute(COMPRESSION_TYPE).asObject(CompressionType.class);
   }

   public Integer blockSize() {
      return attributes.attribute(BLOCK_SIZE).asInteger();
   }

   public Long cacheSize() {
      return attributes.attribute(CACHE_SIZE).asLong();
   }

   public int expiryQueueSize() {
      return attributes.attribute(EXPIRY_QUEUE_SIZE).asInteger();
   }

   public int clearThreshold() {
      return attributes.attribute(CLEAR_THRESHOLD).asInteger();
   }
}
