package org.infinispan.persistence.rest.configuration;

import static org.infinispan.persistence.rest.configuration.Element.REST_STORE;
import static org.infinispan.persistence.rest.configuration.Element.SERVER;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.NestingAttributeSerializer;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.keymappers.WrappedByteArrayOrPrimitiveMapper;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.persistence.rest.metadata.EmbeddedMetadataHelper;

/**
 * RestStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@BuiltBy(RestStoreConfigurationBuilder.class)
@ConfigurationFor(RestStore.class)
@SerializedWith(RestStoreConfigurationSerializer.class)
public class RestStoreConfiguration extends AbstractStoreConfiguration {

   private static NestingAttributeSerializer<Object, RestStoreConfiguration, RestStoreConfigurationBuilder> UNDER_SERVER_ELEMENT = new NestingAttributeSerializer<>(SERVER.getLocalName());

   public static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper", WrappedByteArrayOrPrimitiveMapper.class.getName()).immutable().xmlName("key-to-string-mapper").build();
   public static final AttributeDefinition<String> METADATA_HELPER = AttributeDefinition.builder("metadataHelper", EmbeddedMetadataHelper.class.getName()).immutable().build();
   public static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", null, String.class).immutable().serializer(UNDER_SERVER_ELEMENT).autoPersist(false).build();
   public static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 80).immutable().autoPersist(false).serializer(UNDER_SERVER_ELEMENT).build();
   public static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", "/").immutable().build();
   public static final AttributeDefinition<Boolean> APPEND_CACHE_NAME_TO_PATH = AttributeDefinition.builder("appendCacheNameToPath", false).immutable().build();
   public static final AttributeDefinition<Boolean> RAW_VALUES = AttributeDefinition.builder("rawValues", false).immutable().build();
   public static final AttributeDefinition<Integer> MAX_CONTENT_LENGTH = AttributeDefinition.builder("maxContentLength", 10 * 1024 * 1024).immutable().build();
   private final List<ConfigurationInfo> subElements;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RestStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), KEY2STRING_MAPPER, METADATA_HELPER, HOST, PORT, PATH, APPEND_CACHE_NAME_TO_PATH, RAW_VALUES, MAX_CONTENT_LENGTH);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(REST_STORE.getLocalName());

   private final Attribute<String> key2StringMapper;
   private final Attribute<String> metadataHelper;
   private final Attribute<String> host;
   private final Attribute<Integer> port;
   private final Attribute<String> path;
   private final Attribute<Boolean> appendCacheNameToPath;
   private final Attribute<Boolean> rawValues;
   private final Attribute<Integer> maxContentLength;
   private final ConnectionPoolConfiguration connectionPool;

   public RestStoreConfiguration(AttributeSet attributes,
                                 AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, ConnectionPoolConfiguration connectionPool) {
      super(attributes, async, singletonStore);
      key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
      metadataHelper = attributes.attribute(METADATA_HELPER);
      host = attributes.attribute(HOST);
      port = attributes.attribute(PORT);
      path = attributes.attribute(PATH);
      appendCacheNameToPath = attributes.attribute(APPEND_CACHE_NAME_TO_PATH);
      rawValues = attributes.attribute(RAW_VALUES);
      maxContentLength = attributes.attribute(MAX_CONTENT_LENGTH);
      this.connectionPool = connectionPool;
      this.subElements = new ArrayList<>(super.subElements());
      subElements.add(connectionPool);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public ConnectionPoolConfiguration connectionPool() {
      return connectionPool;
   }

   public String key2StringMapper() {
      return key2StringMapper.get();
   }

   public String metadataHelper() {
      return metadataHelper.get();
   }

   public String host() {
      return host.get();
   }

   public int port() {
      return port.get();
   }

   public String path() {
      return path.get();
   }

   public boolean appendCacheNameToPath() {
      return appendCacheNameToPath.get();
   }

   public boolean rawValues() {
      return rawValues.get();
   }

   public int maxContentLength() {
      return maxContentLength.get();
   }

   @Override
   public String toString() {
      return "RestStoreConfiguration [connectionPool=" + connectionPool + ", attributes=" + attributes + "]";
   }
}
