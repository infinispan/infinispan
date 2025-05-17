package org.infinispan.persistence.rocksdb.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
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
public class RocksDBStoreConfiguration extends AbstractStoreConfiguration<RocksDBStoreConfiguration> {

   static final AttributeDefinition<String> LOCATION = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.PATH, null, String.class).immutable().build();
   public static final AttributeDefinition<CompressionType> COMPRESSION_TYPE = AttributeDefinition.builder(org.infinispan.persistence.rocksdb.configuration.Attribute.COMPRESSION_TYPE, CompressionType.NONE).immutable().autoPersist(false).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RocksDBStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, COMPRESSION_TYPE);
   }

   private final RocksDBExpirationConfiguration expiration;

   public RocksDBStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, RocksDBExpirationConfiguration expiration) {
      super(Element.ROCKSDB_STORE, attributes, async);
      this.expiration = expiration;
   }

   public RocksDBExpirationConfiguration expiration() {
      return expiration;
   }

   public String location() {
      return attributes.attribute(LOCATION).get();
   }

   public String expiredLocation() {
      return expiration.expiredLocation();
   }

   public CompressionType compressionType() {
      return attributes.attribute(COMPRESSION_TYPE).get();
   }
}
