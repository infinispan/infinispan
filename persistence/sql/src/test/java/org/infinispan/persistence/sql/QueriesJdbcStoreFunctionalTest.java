package org.infinispan.persistence.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.SqlManager;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "persistence.jdbc.stringbased.QuerySchemaJdbcStoreFunctionalTest")
public class QueriesJdbcStoreFunctionalTest extends AbstractSQLStoreFunctionalTest {
   public QueriesJdbcStoreFunctionalTest(DatabaseType databaseType, boolean transactionalCache,
         boolean transactionalStore) {
      super(databaseType, transactionalCache, transactionalStore);
   }

   @Factory
   public static Object[] factory() {
      return Arrays.stream(new DatabaseType[]{
            DatabaseType.H2,
//            DatabaseType.POSTGRES,
//            DatabaseType.ORACLE,
//            DatabaseType.MARIA_DB,
//            DatabaseType.DB2,
//            DatabaseType.SQL_SERVER,
            DatabaseType.SQLITE,
//            DatabaseType.SYBASE,
//            DatabaseType.MYSQL
      })
            .flatMap(dt -> Stream.of(
                  new QueriesJdbcStoreFunctionalTest(dt, true, true),
                  new QueriesJdbcStoreFunctionalTest(dt, true, false),
                  new QueriesJdbcStoreFunctionalTest(dt, false, false)
            )).toArray();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      QueriesJdbcStoreConfigurationBuilder storeBuilder = persistence
            .addStore(QueriesJdbcStoreConfigurationBuilder.class)
            .transactional(transactionalStore)
            .preload(preload);
      configureCommonConfiguration(storeBuilder);

      SqlManager manager = SqlManager.fromDatabaseType(DB_TYPE, cacheName, true);

      String KEY_COLUMN = "keycolumn";
      storeBuilder.queriesJdbcConfigurationBuilder()
            .deleteAll("DELETE FROM " + cacheName)
            .size("SELECT COUNT(*) FROM " + cacheName);
      storeBuilder.keyColumns(KEY_COLUMN);
      if (cacheName.equalsIgnoreCase("testPreloadStoredAsBinary")) {
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", name, STREET, city, ZIP, picture, sex, birthdate FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street, city, zip, picture, sex, birthdate FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street", "CITY", "zip", "picture", "sex", "birthdate")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.equalsIgnoreCase("testStoreByteArrays")) {
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", value1 FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", value1 FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value1")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASMOREVALUECOLUMNS")) {
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", name, STREET, city, ZIP, picture, sex, birthdate, value2, value3 FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street, city, zip, picture, sex, birthdate, value2, value3 FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street", "CITY", "zip", "picture", "sex", "birthdate", "value2", "value3")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASMOREKEYCOLUMNS")) {
         // The colum has to be value to match our Key proto schema
         KEY_COLUMN = "value";
         storeBuilder.keyColumns(KEY_COLUMN + ", keycolumn2");
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", keycolumn2, value1 FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", keycolumn2, value1 FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value1")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN + " AND keycolumn2 = :keycolumn2");
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASLESSVALUECOLUMNS")) {
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", name, STREET FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTEMBEDDEDKEY")) {
         storeBuilder.keyColumns("name");
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT name, STREET, city, ZIP, picture, sex, birthdate FROM " + cacheName + " WHERE name = :name")
               .selectAll("SELECT name, street, city, zip, picture, sex, birthdate FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("name"),
                     Arrays.asList("name", "street", "CITY", "zip", "picture", "sex", "birthdate")))
               .delete("DELETE FROM " + cacheName + " WHERE name = :name");
      } else if (cacheName.toUpperCase().startsWith("TESTENUMFORVALUE")) {
         storeBuilder.keyColumns("name");
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT name, sex FROM " + cacheName + " WHERE name = :name")
               .selectAll("SELECT name, sex FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("name"),
                     Arrays.asList("name", "sex")))
               .delete("DELETE FROM " + cacheName + " WHERE name = :name");
      } else if (cacheName.toUpperCase().startsWith("TESTENUMFORKEY")) {
         storeBuilder.keyColumns("sex");
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT name, sex FROM " + cacheName + " WHERE sex = :sex")
               .selectAll("SELECT name, sex FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("sex"),
                     Arrays.asList("name", "sex")))
               .delete("DELETE FROM " + cacheName + " WHERE sex = :sex");
      } else {
         storeBuilder.queriesJdbcConfigurationBuilder()
               .select("SELECT " + KEY_COLUMN + ", value1 FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", value1 FROM " + cacheName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value1")))
               .delete("DELETE FROM " + cacheName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      }

      createTable(cacheName, storeBuilder.getConnectionFactory());

      return persistence;
   }
}
