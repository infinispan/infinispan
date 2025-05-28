package org.infinispan.persistence.sql;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.SqlManager;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfigurationBuilder;
import org.testng.SkipException;
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

      DatabaseType[] databases;
      if(DATABASE == null) {
         databases = new DatabaseType[]{
                 DatabaseType.H2,
                 DatabaseType.SQLITE
         };
      } else {
         databases = databasesFromSystemProperty.keySet().stream().toArray(DatabaseType[] :: new);
      }

      return Arrays.stream(databases)
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

      // Just prepend the first letter of the Test to make the tables unique so we can run them in parallel
      String tableName = getClass().getSimpleName().subSequence(0, 1) + cacheName;

      SqlManager manager = SqlManager.fromDatabaseType(DB_TYPE, tableName, true);

      String KEY_COLUMN = "keycolumn";
      storeBuilder.queries()
            .deleteAll("DELETE FROM " + tableName)
            .size("SELECT COUNT(*) FROM " + tableName);
      storeBuilder.keyColumns(KEY_COLUMN);
      if ("testKeyWithNullFields".equalsIgnoreCase(cacheName)) {
         String upsert = manager.getUpsertStatement(Arrays.asList("street", "city"), Arrays.asList("name", "street", "city", "zip"));
         storeBuilder.keyColumns("street, city");
         storeBuilder.queries()
               .select("SELECT name, street, city, zip FROM " + tableName + " WHERE (street = :street OR :street IS NULL) AND city = :city")
               .selectAll("SELECT name, street, city, zip FROM " + tableName)
               .upsert(upsert)
               .delete("DELETE FROM " + tableName + " WHERE (street = :street OR :street IS NULL) AND city = :city");
      } else if ("testPreloadStoredAsBinary".equalsIgnoreCase(cacheName)) {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", name, STREET, city, ZIP, picture, sex, birthdate, accepted_tos, moneyOwned, moneyOwed, decimalField, realField FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street, city, zip, picture, sex, birthdate, accepted_tos, moneyOwned, moneyOwed, decimalField, realField FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street", "CITY", "zip", "picture", "sex", "birthdate", "accepted_tos", "moneyOwned", "moneyOwed", "decimalField", "realField")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if ("testStoreByteArrays".equalsIgnoreCase(cacheName)) {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", value1 FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", value1 FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value1")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASMOREVALUECOLUMNS")) {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", name, STREET, city, ZIP, picture, sex, birthdate, value2, value3 FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street, city, zip, picture, sex, birthdate, value2, value3 FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street", "CITY", "zip", "picture", "sex", "birthdate", "value2", "value3")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASMOREKEYCOLUMNS")) {
         // The colum has to be value1 to match our Key proto schema
         KEY_COLUMN = "value1";
         storeBuilder.keyColumns(KEY_COLUMN + ", keycolumn2");
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", keycolumn2, value2 FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", keycolumn2, value2 FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value2")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN + " AND keycolumn2 = :keycolumn2");
      } else if (cacheName.toUpperCase().startsWith("TESTDBHASLESSVALUECOLUMNS")) {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", name, STREET FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", name, street FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "name", "street")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else if (cacheName.toUpperCase().startsWith("TESTEMBEDDED")) {
         storeBuilder.keyColumns("name");
         storeBuilder.queries()
               .select("SELECT name, STREET, city, ZIP, picture, sex, birthdate FROM " + tableName + " WHERE name = :name")
               .selectAll("SELECT name, street, city, zip, picture, sex, birthdate FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("name"),
                     Arrays.asList("name", "street", "CITY", "zip", "picture", "sex", "birthdate")))
               .delete("DELETE FROM " + tableName + " WHERE name = :name");
      } else if (cacheName.toUpperCase().startsWith("TESTENUMFORVALUE")) {
         storeBuilder.keyColumns("name");
         storeBuilder.queries()
               .select("SELECT name, sex FROM " + tableName + " WHERE name = :name")
               .selectAll("SELECT name, sex FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("name"),
                     Arrays.asList("name", "sex")))
               .delete("DELETE FROM " + tableName + " WHERE name = :name");
      } else if (cacheName.toUpperCase().startsWith("TESTENUMFORKEY")) {
         storeBuilder.keyColumns("sex");
         storeBuilder.queries()
               .select("SELECT name, sex FROM " + tableName + " WHERE sex = :sex")
               .selectAll("SELECT name, sex FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList("sex"),
                     Arrays.asList("name", "sex")))
               .delete("DELETE FROM " + tableName + " WHERE sex = :sex");
      } else if ("TESTNUMERICCOLUMNS".equalsIgnoreCase(cacheName)) {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", simpleLong, simpleFloat, simpleDouble, largeInteger FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", simpleLong, simpleFloat, simpleDouble, largeInteger FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "simpleLong", "simpleFloat", "simpleDouble", "largeInteger")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      } else {
         storeBuilder.queries()
               .select("SELECT " + KEY_COLUMN + ", value1 FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN)
               .selectAll("SELECT " + KEY_COLUMN + ", value1 FROM " + tableName)
               .upsert(manager.getUpsertStatement(Collections.singletonList(KEY_COLUMN),
                     Arrays.asList(KEY_COLUMN, "value1")))
               .delete("DELETE FROM " + tableName + " WHERE " + KEY_COLUMN + " = :" + KEY_COLUMN);
      }

      createTable(cacheName, tableName, storeBuilder.getConnectionFactory());

      return persistence;
   }

   @Override
   public void testNumericColumns(Method m) {
      if (DB_TYPE.equals(DatabaseType.SQLITE))
         throw new SkipException("Query store not running with SQLite to check numerics");

      super.testNumericColumns(m);
   }
}
