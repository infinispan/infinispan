package org.infinispan.persistence.rest.configuration;

import static org.infinispan.persistence.rest.configuration.Element.REST_STORE;

import java.util.ArrayList;
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

   public static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper", WrappedByteArrayOrPrimitiveMapper.class.getName()).immutable().xmlName("key-to-string-mapper").build();
   public static final AttributeDefinition<String> METADATA_HELPER = AttributeDefinition.builder("metadataHelper", EmbeddedMetadataHelper.class.getName()).immutable().build();
   public static final AttributeDefinition<String> CACHE_NAME = AttributeDefinition.builder("cacheName", null, String.class).immutable().build();
   public static final AttributeDefinition<Boolean> RAW_VALUES = AttributeDefinition.builder("rawValues", false).immutable().build();
   public static final AttributeDefinition<Integer> MAX_CONTENT_LENGTH = AttributeDefinition.builder("maxContentLength", 10 * 1024 * 1024).immutable().build();
   private final List<ConfigurationInfo> subElements;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RestStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), KEY2STRING_MAPPER, METADATA_HELPER, CACHE_NAME, RAW_VALUES, MAX_CONTENT_LENGTH);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(REST_STORE.getLocalName());

   private final Attribute<String> key2StringMapper;
   private final Attribute<String> metadataHelper;
   private final Attribute<String> cacheName;
   private final Attribute<Boolean> rawValues;
   private final Attribute<Integer> maxContentLength;
   private final ConnectionPoolConfiguration connectionPool;
   private final RemoteServerConfiguration remoteServer;

   public RestStoreConfiguration(AttributeSet attributes,
                                 AsyncStoreConfiguration async, ConnectionPoolConfiguration connectionPool,
                                 RemoteServerConfiguration remoteServer) {
      super(attributes, async);
      key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
      metadataHelper = attributes.attribute(METADATA_HELPER);
      cacheName = attributes.attribute(CACHE_NAME);
      rawValues = attributes.attribute(RAW_VALUES);
      maxContentLength = attributes.attribute(MAX_CONTENT_LENGTH);
      this.connectionPool = connectionPool;
      this.remoteServer = remoteServer;
      this.subElements = new ArrayList<>(super.subElements());
      subElements.add(remoteServer);
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

   public RemoteServerConfiguration remoteServer() {
      return remoteServer;
   }

   public String key2StringMapper() {
      return key2StringMapper.get();
   }

   public String metadataHelper() {
      return metadataHelper.get();
   }

   public String host() {
      return remoteServer.host();
   }

   public int port() {
      return remoteServer.port();
   }

   public String cacheName() {
      return cacheName.get();
   }

   public boolean rawValues() {
      return rawValues.get();
   }

   public int maxContentLength() {
      return maxContentLength.get();
   }

   @Override
   public String toString() {
      return "RestStoreConfiguration{" +
            "connectionPool=" + connectionPool +
            ", remoteServer=" + remoteServer +
            ", attributes=" + attributes +
            '}';
   }


}
