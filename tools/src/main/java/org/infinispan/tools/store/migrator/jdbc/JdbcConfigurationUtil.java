package org.infinispan.tools.store.migrator.jdbc;

import static org.infinispan.tools.store.migrator.Element.BINARY;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.store.migrator.Element.DATA;
import static org.infinispan.tools.store.migrator.Element.DB;
import static org.infinispan.tools.store.migrator.Element.DIALECT;
import static org.infinispan.tools.store.migrator.Element.DISABLE_INDEXING;
import static org.infinispan.tools.store.migrator.Element.DISABLE_UPSERT;
import static org.infinispan.tools.store.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.store.migrator.Element.ID;
import static org.infinispan.tools.store.migrator.Element.KEY_TO_STRING_MAPPER;
import static org.infinispan.tools.store.migrator.Element.MAJOR_VERSION;
import static org.infinispan.tools.store.migrator.Element.MINOR_VERSION;
import static org.infinispan.tools.store.migrator.Element.NAME;
import static org.infinispan.tools.store.migrator.Element.PASSWORD;
import static org.infinispan.tools.store.migrator.Element.STRING;
import static org.infinispan.tools.store.migrator.Element.TABLE;
import static org.infinispan.tools.store.migrator.Element.TABLE_NAME_PREFIX;
import static org.infinispan.tools.store.migrator.Element.TIMESTAMP;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.USERNAME;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.StoreType;

public class JdbcConfigurationUtil {

   static JdbcStringBasedStoreConfiguration getStoreConfig(StoreProperties props) {
      JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      configureStore(props, builder);
      return builder.create();
   }

   public static JdbcStringBasedStoreConfigurationBuilder configureStore(StoreProperties props, JdbcStringBasedStoreConfigurationBuilder builder) {
      StoreType type = props.storeType();
      configureDbMeta(props, builder);
      if (type == StoreType.MIXED || type == StoreType.STRING) {
         createTableConfig(props, STRING, builder);
         String mapper = props.get(KEY_TO_STRING_MAPPER);
         if (mapper != null)
            builder.key2StringMapper(props.get(KEY_TO_STRING_MAPPER));
      }

      if (type == StoreType.MIXED || type == StoreType.BINARY) {
         createTableConfig(props, BINARY, builder);
      }
      createConnectionConfig(props, builder);
      builder.validate();
      return builder;
   }

   static TableManipulationConfiguration createTableConfig(StoreProperties props, Element tableType, JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
      return storeBuilder.table()
            .createOnStart(props.isTargetStore())
            .tableNamePrefix(props.get(TABLE, tableType, TABLE_NAME_PREFIX))
            .idColumnName(props.get(TABLE, tableType, ID, NAME))
            .idColumnType(props.get(TABLE, tableType, ID, TYPE))
            .dataColumnName(props.get(TABLE, tableType, DATA, NAME))
            .dataColumnType(props.get(TABLE, tableType, DATA, TYPE))
            .timestampColumnName(props.get(TABLE, tableType, TIMESTAMP, NAME))
            .timestampColumnType(props.get(TABLE, tableType, TIMESTAMP, TYPE))
            .create();
   }

   private static void configureDbMeta(StoreProperties props, JdbcStringBasedStoreConfigurationBuilder builder) {
      props.required(DIALECT);
      DatabaseType type = DatabaseType.valueOf(props.get(DIALECT).toUpperCase());
      builder.dialect(type);

      String prop;
      if ((prop = props.get(DB, MAJOR_VERSION)) != null) {
         builder.dbMajorVersion(new Integer(prop));
      }

      if ((prop = props.get(DB, MINOR_VERSION)) != null) {
         builder.dbMinorVersion(new Integer(prop));
      }

      String disableUpsert = props.get(DB, DISABLE_UPSERT);
      boolean upsert = Boolean.parseBoolean(disableUpsert);
      if (upsert)
         builder.addProperty(TableManagerFactory.UPSERT_DISABLED, disableUpsert);

      String disableIndexing = props.get(DB, DISABLE_INDEXING);
      boolean indexing = Boolean.parseBoolean(disableIndexing);
      if (indexing)
         builder.addProperty(TableManagerFactory.INDEXING_DISABLED, disableIndexing);
   }

   private static void createConnectionConfig(StoreProperties props, JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
      props.required(props.key(CONNECTION_POOL, CONNECTION_URL));
      props.required(props.key(CONNECTION_POOL, DRIVER_CLASS));

      storeBuilder.connectionPool()
            .connectionUrl(props.get(CONNECTION_POOL, CONNECTION_URL))
            .driverClass(props.get(CONNECTION_POOL, DRIVER_CLASS))
            .username(props.get(CONNECTION_POOL, USERNAME))
            .password(props.get(CONNECTION_POOL, PASSWORD))
            .create();
   }
}
