package org.infinispan.persistence.rest.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.persistence.rest.metadata.EmbeddedMetadataHelper;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.util.logging.LogFactory;

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
   private String key2StringMapper = MarshalledValueOrPrimitiveMapper.class.getName();
   private String metadataHelper = EmbeddedMetadataHelper.class.getName();
   private String path = "/";
   private String host;
   private int port = 80;
   private boolean appendCacheNameToPath = false;
   private boolean rawValues = false;

   public RestStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
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
      this.host = host;
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }


   @Override
   public RestStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(String metadataHelper) {
      this.metadataHelper = metadataHelper;
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper) {
      this.metadataHelper = metadataHelper.getName();
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder path(String path) {
      this.path = path;
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath) {
      this.appendCacheNameToPath = appendCacheNameToPath;
      return this;
   }

   @Override
   public RestStoreConfigurationBuilder rawValues(boolean rawValues) {
      this.rawValues = rawValues;
      return this;
   }

   @Override
   public RestStoreConfiguration create() {
      return new RestStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                             singletonStore.create(), preload, shared, properties, connectionPool.create(),
                                             key2StringMapper, metadataHelper, host, port, path, appendCacheNameToPath, rawValues);
   }

   @Override
   public RestStoreConfigurationBuilder read(RestStoreConfiguration template) {
      super.read(template);

      this.connectionPool.read(template.connectionPool());
      this.host = template.host();
      this.port = template.port();
      this.path = template.path();
      this.appendCacheNameToPath = template.appendCacheNameToPath();
      this.key2StringMapper = template.key2StringMapper();
      this.metadataHelper = template.metadataHelper();
      this.rawValues = template.rawValues();

      return this;
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      if (host == null) {
         throw log.hostNotSpecified();
      }
      if (!path.endsWith("/")) {
         path = path + "/";
      }
   }
}
