package org.infinispan.tools.store.migrator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.table.management.DbMetaData;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.tools.store.migrator.marshaller.LegacyVersionAwareMarshaller;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class MigratorConfiguration {
   final String cacheName;
   final StoreType storeType;
   private final Properties properties;
   private final boolean sourceStore;
   private final Element orientation;
   private final ClassLoader classLoader;
   private DbMetaData dbMetaData;
   private TableManipulationConfiguration stringTable = null;
   private TableManipulationConfiguration binaryTable = null;
   private ConnectionFactoryConfiguration connectionConfig;
   private JdbcStringBasedStoreConfigurationBuilder jdbcConfigBuilder;
   private TwoWayKey2StringMapper key2StringMapper;
   private StreamingMarshaller marshaller;
   private Map<Integer, AdvancedExternalizer<?>> externalizerMap;

   MigratorConfiguration(boolean sourceStore, Properties properties) {
      this.properties = properties;
      this.sourceStore = sourceStore;
      this.orientation = sourceStore ? Element.SOURCE : Element.TARGET;
      this.classLoader = MigratorConfiguration.class.getClassLoader();

      requiredProps(propKey(Element.CACHE_NAME), propKey(Element.TYPE));
      this.cacheName = property(Element.CACHE_NAME);
      this.storeType = StoreType.valueOf(property(Element.TYPE).toUpperCase());
      initStoreConfig();
   }

   private void requiredProps(String... required) {
      for (String prop : required) {
         if (properties.get(prop) == null) {
            String msg = String.format("The property %s must be specified.", prop);
            throw new CacheConfigurationException(msg);
         }
      }
   }

   private void initStoreConfig() {
      JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      dbMetaData = createDbMeta(builder);
      connectionConfig = createConnectionConfig(builder);
      marshaller = createMarshaller();
      if (sourceStore) {
         if (storeType == StoreType.MIXED || storeType == StoreType.STRING) {
            stringTable = createTableConfig(Element.STRING, builder);
            key2StringMapper = createTwoWayMapper();
         }

         if (storeType == StoreType.MIXED || storeType == StoreType.BINARY)
            binaryTable = createTableConfig(Element.BINARY, builder);
      } else {
         key2StringMapper = createTwoWayMapper();
         builder.key2StringMapper(key2StringMapper.getClass());
         stringTable = createTableConfig(Element.STRING, builder);
         builder.transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      }
      builder.validate();
      jdbcConfigBuilder = builder;
   }

   private DbMetaData createDbMeta(JdbcStringBasedStoreConfigurationBuilder builder) {
      requiredProps(propKey(Element.DIALECT));
      String prop;
      DatabaseType type = DatabaseType.valueOf(property(Element.DIALECT).toUpperCase());
      builder.dialect(type);

      Integer major = null;
      if ((prop = property(Element.DB, Element.MAJOR_VERSION)) != null) {
         major = new Integer(prop);
         builder.dbMajorVersion(major);
      }

      Integer minor = null;
      if ((prop = property(Element.DB, Element.MINOR_VERSION)) != null) {
         minor = new Integer(prop);
         builder.dbMinorVersion(minor);
      }

      String disableUpsert = property(Element.DB, Element.DISABLE_UPSERT);
      boolean upsert = Boolean.parseBoolean(disableUpsert);
      if (upsert)
         builder.addProperty(TableManagerFactory.UPSERT_DISABLED, disableUpsert);

      String disableIndexing = property(Element.DB, Element.DISABLE_INDEXING);
      boolean indexing = Boolean.parseBoolean(disableIndexing);
      if (indexing)
         builder.addProperty(TableManagerFactory.INDEXING_DISABLED, disableIndexing);

      return new DbMetaData(type, major, minor, upsert, indexing);
   }

   private TableManipulationConfiguration createTableConfig(Element tableType, JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
      boolean createOnStart = orientation == Element.TARGET;
      return storeBuilder.table()
            .createOnStart(createOnStart)
            .tableNamePrefix(property(Element.TABLE, tableType, Element.TABLE_NAME_PREFIX))
            .idColumnName(property(Element.TABLE, tableType, Element.ID, Element.NAME))
            .idColumnType(property(Element.TABLE, tableType, Element.ID, Element.TYPE))
            .dataColumnName(property(Element.TABLE, tableType, Element.DATA, Element.NAME))
            .dataColumnType(property(Element.TABLE, tableType, Element.DATA, Element.TYPE))
            .timestampColumnName(property(Element.TABLE, tableType, Element.TIMESTAMP, Element.NAME))
            .timestampColumnType(property(Element.TABLE, tableType, Element.TIMESTAMP, Element.TYPE))
            .create();
   }

   private PooledConnectionFactoryConfiguration createConnectionConfig(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
      requiredProps(propKey(Element.CONNECTION_POOL, Element.CONNECTION_URL), propKey(Element.CONNECTION_POOL, Element.DRIVER_CLASS));
      return storeBuilder.connectionPool()
            .connectionUrl(property(Element.CONNECTION_POOL, Element.CONNECTION_URL))
            .driverClass(property(Element.CONNECTION_POOL, Element.DRIVER_CLASS))
            .username(property(Element.CONNECTION_POOL, Element.USERNAME))
            .password(property(Element.CONNECTION_POOL, Element.PASSWORD))
            .create();
   }

   private TwoWayKey2StringMapper createTwoWayMapper() {
      String mapperClass = property(Element.KEY_TO_STRING_MAPPER);
      if (mapperClass != null) {
         ClassLoader classLoader = MigratorConfiguration.class.getClassLoader();
         try {
            return (TwoWayKey2StringMapper) Util.loadClass(mapperClass, classLoader).newInstance();
         } catch (IllegalAccessException | InstantiationException e) {
            throw new CacheConfigurationException(String.format("Unabled to load TwoWayKey2StringMapper '%s' for %s store",
                  mapperClass, orientation), e);
         }
      }
      return new DefaultTwoWayKey2StringMapper();
   }

   private StreamingMarshaller createMarshaller() {
      MarshallerType marshallerType = MarshallerType.CURRENT;
      String marshallerTypeProp = property(Element.MARSHALLER, Element.TYPE);
      if (marshallerTypeProp != null)
         marshallerType = MarshallerType.valueOf(property(Element.MARSHALLER, Element.TYPE).toUpperCase());

      switch (marshallerType) {
         case CURRENT:
            externalizerMap = getExternalizersFromProps();
            if (orientation == Element.TARGET)
               return null;

            GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder()
                  .defaultCacheName(cacheName);
            addExternalizersToConfig(globalConfig.serialization());

            EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build(), new ConfigurationBuilder().build());
            return manager.getCache().getAdvancedCache().getComponentRegistry().getComponent(StreamingMarshaller.class);
         case CUSTOM:
            String marshallerClass = property(Element.MARSHALLER, Element.CLASS);
            if (marshallerClass == null)
               throw new CacheConfigurationException(
                     String.format("The property %s.%s must be set if a custom marshaller type is specified", Element.MARSHALLER, Element.CLASS));

            try {
               return (StreamingMarshaller) Util.loadClass(marshallerClass, classLoader).newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
               throw new CacheConfigurationException(String.format("Unabled to load StreamingMarshaller '%s' for %s store",
                     marshallerClass, orientation), e);
            }
         case LEGACY:
            if (orientation != Element.SOURCE)
               throw new CacheConfigurationException("The legacy marshaller can only be specified for source stores.");
            return new LegacyVersionAwareMarshaller(getExternalizersFromProps());
         default:
            throw new IllegalStateException("Unexpected marshaller type");
      }
   }

   // Expects externalizer string to be a comma-separated list of "<id>:<class>"
   private Map<Integer, AdvancedExternalizer<?>> getExternalizersFromProps() {
      Map<Integer, AdvancedExternalizer<?>> map = new HashMap<>();
      String externalizers = property(Element.MARSHALLER, Element.EXTERNALIZERS);
      if (externalizers != null) {
         for (String ext : externalizers.split(",")) {
            String[] extArray = ext.split(":");
            String className = extArray.length > 1 ? extArray[1] : extArray[0];
            AdvancedExternalizer<?> instance = Util.getInstance(className, classLoader);
            int id = extArray.length > 1 ? new Integer(extArray[0]) : instance.getId();
            map.put(id, instance);
         }
      }
      return map;
   }

   ConnectionFactoryConfiguration getConnectionConfig() {
      return connectionConfig;
   }

   DbMetaData getDbMeta() {
      return dbMetaData;
   }

   TableManipulationConfiguration getStringTable() {
      return stringTable;
   }

   TableManipulationConfiguration getBinaryTable() {
      return binaryTable;
   }

   JdbcStringBasedStoreConfigurationBuilder getJdbcConfigBuilder() {
      return jdbcConfigBuilder;
   }

   TwoWayKey2StringMapper getKey2StringMapper() {
      return key2StringMapper;
   }

   StreamingMarshaller getMarshaller() {
      return marshaller;
   }

   void addExternalizersToConfig(SerializationConfigurationBuilder builder) {
      if (externalizerMap == null)
         return;

      for (Map.Entry<Integer, AdvancedExternalizer<?>> entry : externalizerMap.entrySet())
         builder.addAdvancedExternalizer(entry.getKey(), entry.getValue());
   }

   private String property(Element... elements) {
      String key = propKey(elements);
      return properties.getProperty(key);
   }

   private String propKey(Element... elements) {
      StringBuilder sb = new StringBuilder(orientation.toString().toLowerCase());
      sb.append(".");
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return sb.toString();
   }
}
