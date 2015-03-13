package org.infinispan.persistence.rest.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.persistence.rest.metadata.EmbeddedMetadataHelper;

import java.util.Properties;

/**
 * RestStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@BuiltBy(RestStoreConfigurationBuilder.class)
@ConfigurationFor(RestStore.class)
public class RestStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper", MarshalledValueOrPrimitiveMapper.class.getName()).immutable().build();
   static final AttributeDefinition<String> METADATA_HELPER = AttributeDefinition.builder("metadataHelper", EmbeddedMetadataHelper.class.getName()).immutable().build();
   static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 80).immutable().build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", "/").immutable().build();
   static final AttributeDefinition<Boolean> APPEND_CACHE_NAME_TO_PATH = AttributeDefinition.builder("appendCacheNameToPath", false).immutable().build();
   static final AttributeDefinition<Boolean> RAW_VALUES = AttributeDefinition.builder("rawValues", false).immutable().build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(RestStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), KEY2STRING_MAPPER, METADATA_HELPER, HOST, PORT, PATH, APPEND_CACHE_NAME_TO_PATH, RAW_VALUES);
   }
   private final ConnectionPoolConfiguration connectionPool;

   public RestStoreConfiguration(AttributeSet attributes,
                                 AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, ConnectionPoolConfiguration connectionPool) {
      super(attributes, async, singletonStore);
      this.connectionPool = connectionPool;
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public String key2StringMapper() {
      return attributes.attribute(KEY2STRING_MAPPER).asString();
   }

   public String metadataHelper() {
      return attributes.attribute(METADATA_HELPER).asString();
   }

   public String host() {
      return attributes.attribute(HOST).asString();
   }

   public int port() {
      return attributes.attribute(PORT).asInteger();
   }

   public String path() {
      return attributes.attribute(PATH).asString();
   }

   public boolean appendCacheNameToPath() {
      return attributes.attribute(APPEND_CACHE_NAME_TO_PATH).asBoolean();
   }

   public boolean rawValues() {
      return attributes.attribute(RAW_VALUES).asBoolean();
   }

   @Override
   public String toString() {
      return "RestStoreConfiguration [connectionPool=" + connectionPool + ", attributes=" + attributes + "]";
   }
}
