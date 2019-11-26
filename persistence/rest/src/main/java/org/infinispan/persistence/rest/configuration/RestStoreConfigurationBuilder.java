package org.infinispan.persistence.rest.configuration;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.CACHE_NAME;
import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.KEY2STRING_MAPPER;
import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.MAX_CONTENT_LENGTH;
import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.METADATA_HELPER;
import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.RAW_VALUES;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * RestStoreConfigurationBuilder. Configures a {@link org.infinispan.persistence.rest.RestStore}
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class RestStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RestStoreConfiguration, RestStoreConfigurationBuilder>
      implements RestStoreConfigurationChildBuilder<RestStoreConfigurationBuilder>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(RestStoreConfigurationBuilder.class, Log.class);
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private final RemoteServerConfigurationBuilder remoteServer;
   private final List<ConfigurationBuilderInfo> subElements;

   public RestStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RestStoreConfiguration.attributeDefinitionSet());
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
      remoteServer = new RemoteServerConfigurationBuilder();
      subElements = Arrays.asList(connectionPool, remoteServer);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RestStoreConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   @Override
   public RestStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RestStoreConfigurationBuilder host(String host) {
      remoteServer.host(host);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      attributes.attribute(KEY2STRING_MAPPER).set(key2StringMapper);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass) {
      attributes.attribute(KEY2STRING_MAPPER).set(klass.getName());
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(String metadataHelper) {
      attributes.attribute(METADATA_HELPER).set(metadataHelper);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper) {
      metadataHelper(metadataHelper.getName());
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder cacheName(String cacheName) {
      attributes.attribute(CACHE_NAME).set(cacheName);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder port(int port) {
      remoteServer.port(port);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder rawValues(boolean rawValues) {
      attributes.attribute(RAW_VALUES).set(rawValues);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder maxContentLength(int maxContentLength) {
      attributes.attribute(MAX_CONTENT_LENGTH).set(maxContentLength);
      return this;
   }

   @Override
   public RestStoreConfiguration create() {
      return new RestStoreConfiguration(attributes.protect(), async.create(), connectionPool.create(), remoteServer.create());
   }

   @Override
   public RestStoreConfigurationBuilder read(RestStoreConfiguration template) {
      super.read(template);
      this.connectionPool.read(template.connectionPool());
      this.remoteServer.read(template.remoteServer());
      return this;
   }

   @Override
   public void validate() {
      Boolean segmented = attributes.attribute(SEGMENTED).get();
      if (segmented == null || segmented) {
         throw log.storeDoesNotSupportBeingSegmented(RestStore.class.getSimpleName());
      }
      this.connectionPool.validate();
      this.remoteServer.validate();
   }
}
