package org.infinispan.persistence.rest.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.persistence.rest.metadata.EmbeddedMetadataHelper;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.persistence.rest.configuration.RestStoreConfiguration.*;
/**
 * RestStoreConfigurationBuilder. Configures a {@link org.infinispan.persistence.rest.RestStore}
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class RestStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RestStoreConfiguration, RestStoreConfigurationBuilder> implements
                                                                                                                                            RestStoreConfigurationChildBuilder<RestStoreConfigurationBuilder> {
   private static final Log log = LogFactory.getLog(RestStoreConfigurationBuilder.class, Log.class);
   private final ConnectionPoolConfigurationBuilder connectionPool;

   public RestStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RestStoreConfiguration.attributeDefinitionSet());
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
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
      attributes.attribute(HOST).set(host);
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
   public RestStoreConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder port(int port) {
      attributes.attribute(PORT).set(port);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath) {
      attributes.attribute(APPEND_CACHE_NAME_TO_PATH).set(appendCacheNameToPath);
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder rawValues(boolean rawValues) {
      attributes.attribute(RAW_VALUES).set(rawValues);
      return this;
   }

   @Override
   public RestStoreConfiguration create() {
      return new RestStoreConfiguration(attributes.protect(), async.create(),
                                             singletonStore.create(), connectionPool.create());
   }

   @Override
   public RestStoreConfigurationBuilder read(RestStoreConfiguration template) {
      super.read(template);
      this.connectionPool.read(template.connectionPool());

      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      if (attributes.attribute(HOST).get() == null) {
         throw log.hostNotSpecified();
      }
      String path = attributes.attribute(PATH).get();
      if (!path.endsWith("/")) {
         path(path + "/");
      }
   }
}
