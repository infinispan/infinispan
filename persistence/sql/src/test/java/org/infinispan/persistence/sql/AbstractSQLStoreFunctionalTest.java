package org.infinispan.persistence.sql;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.sql.configuration.AbstractSchemaJdbcConfigurationBuilder;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Sex;
import org.infinispan.transaction.TransactionMode;
import org.postgresql.Driver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class AbstractSQLStoreFunctionalTest extends BaseStoreFunctionalTest {

   protected final DatabaseType DB_TYPE;
   protected final boolean transactionalCache;
   protected final boolean transactionalStore;

   protected String tmpDirectory;
   protected Consumer<AbstractSchemaJdbcConfigurationBuilder<?, ?>> schemaConsumer;

   public AbstractSQLStoreFunctionalTest(DatabaseType databaseType, boolean transactionalCache,
         boolean transactionalStore) {
      this.DB_TYPE = databaseType;
      this.transactionalCache = transactionalCache;
      this.transactionalStore = transactionalStore;
   }

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      new File(tmpDirectory).mkdirs();
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   protected void createBeforeMethod() throws Exception {
      schemaConsumer = null;
      super.createBeforeMethod();
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   // DB table is denormalized when read
   @Override
   protected Person createEmptyPerson(String name) {
      return new Person(name, new Address(), null, null, null);
   }

   @Override
   protected String parameters() {
      return "[" + DB_TYPE + ", transactionalCache=" + transactionalCache + ", transactionalStore=" + transactionalStore
            + "]";
   }

   @Override
   protected ConfigurationBuilder getDefaultCacheConfiguration() {
      ConfigurationBuilder builder = super.getDefaultCacheConfiguration();
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      if (transactionalCache) {
         builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      return builder;
   }

   @Override
   public void testPreloadStoredAsBinary() {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      super.testPreloadStoredAsBinary();
   }

   @Test(enabled = false, description = "Expiration not supported")
   @Override
   public void testPreloadAndExpiry() {
      // Expiration not supported
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testTwoCachesSameCacheStore() {
      // Stores are always shared
   }

   @Override
   public void testRemoveCacheWithPassivation() {
      if (!transactionalStore) {
         super.testRemoveCacheWithPassivation();
      }
   }

   public void testRollback() throws SystemException, NotSupportedException {
      if (!transactionalCache) {
         return;
      }
      String cacheName = "testRollback";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      cacheManager.defineConfiguration(cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      String key = "rollback-test";
      assertNull(cache.get(key));

      TransactionManager manager = cache.getAdvancedCache().getTransactionManager();

      String value = "the-value";
      manager.begin();
      cache.put(key, value);
      assertEquals(value, cache.get(key));
      manager.rollback();

      assertNull(cache.get(key));
   }

   public void testDBHasMoreKeyColumnsWithKeySchema(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .keyMessageName("Key")
                  .packageName("org.infinispan.test.core");
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            ".*Primary key (?i)(KEYCOLUMN2) was not found.*",
            () -> testSimpleGetAndPut(m.getName(), new Key("mykey"), "value"));
   }

   public void testDBHasMoreKeyColumnsWithNoKeySchema(Method m) {
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            ".*Primary key has multiple columns .*",
            () -> testSimpleGetAndPut(m.getName(), "key", "value"));
   }

//   public void testDBHasLessKeyColumnsWithSchema(Method m) {
//      // TODO: add new schema key with 2 columns
//      schemaConsumer = builder ->
//            builder.schemaJdbcConfigurationBuilder()
//                  .embeddedKey(false)
//                  .keyMessageName("Key")
//                  .packageName("org.infinispan.test.core");
//      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
//            CacheConfigurationException.class,
//            "Primary key has multiple columns .*",
//            () -> testSimpleGetAndPut(m.getName(), "key", "value"));
//   }

   public void testDBHasMoreValueColumnsWithValueSchema(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            ".*Additional value columns .* found that were not part of the schema,.*",
            () -> testSimpleGetAndPut(m.getName(), "key", new Person("man2")));
   }

   public void testDBHasMoreValueColumnsWithNoValueSchema(Method m) {
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            ".*Multiple non key columns but no value message schema defined.*",
            () -> testSimpleGetAndPut(m.getName(), "key", "value"));
   }

   public void testDBHasLessValueColumnsWithSchema(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "key", new Person("joe"));
   }

   public void testEmbeddedKey(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(true)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "joe", new Person("joe"));
   }

   public void testEnumForKey(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .keyMessageName("Sex")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), Sex.FEMALE, "samantha");
   }

   public void testEnumForValue(Method m) {
      schemaConsumer = builder ->
            builder.schemaJdbcConfigurationBuilder()
                  .embeddedKey(false)
                  .messageName("Sex")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "samantha", Sex.FEMALE);
   }

   private void testSimpleGetAndPut(String cacheName, Object key, Object value) {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      cacheManager.defineConfiguration(cacheName, cb.build());

      Cache<Object, Object> cache = cacheManager.getCache(cacheName);

      assertNull(cache.get(key));

      cache.put(key, value);

      assertEquals(value, cache.get(key));
   }

   protected void configureCommonConfiguration(AbstractSchemaJdbcConfigurationBuilder<?, ?> builder) {

      if (schemaConsumer != null) {
         schemaConsumer.accept(builder);
      }

      switch (DB_TYPE) {
         case POSTGRES:
            builder.connectionPool()
                  .driverClass(Driver.class)
                  .connectionUrl("jdbc:postgresql://172.17.0.1:5432/test1")
                  .username("postgres")
                  .password("example");
            break;
         case ORACLE:
            builder.connectionPool()
                  .driverClass("oracle.jdbc.OracleDriver")
                  .connectionUrl("jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(ADDRESS=(PROTOCOL=TCP)(HOST=oracle-19c-rac-01.hosts.mwqe.eng.bos.redhat.com)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=oracle-19c-rac-02.hosts.mwqe.eng.bos.redhat.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=dballo)))")
                  .username("dballo10")
                  .password("dballo10");
            break;
         case MARIA_DB:
            builder.connectionPool()
                  .driverClass("org.mariadb.jdbc.Driver")
                  .connectionUrl("jdbc:mariadb://mariadb-101.hosts.mwqe.eng.bos.redhat.com:3306/dballo12")
                  .username("dballo12")
                  .password("dballo12");
            break;
         case DB2:
            builder.connectionPool()
                  .driverClass("com.ibm.db2.jcc.DB2Driver")
                  .connectionUrl("jdbc:db2://db2-111.hosts.mwqe.eng.bos.redhat.com:50000/dballo")
                  .username("dballo16")
                  .password("dballo16");
            break;
         case MYSQL:
            builder.connectionPool()
                  .driverClass("com.mysql.cj.jdbc.Driver")
                  .connectionUrl("jdbc:mysql://mysql-80.hosts.mwqe.eng.bos.redhat.com:3306/dballo09")
                  .username("dballo09")
                  .password("dballo09");
            break;
         case SQLITE:
            builder.connectionPool()
                  .driverClass("org.sqlite.JDBC")
                  .connectionUrl("jdbc:sqlite:" + tmpDirectory + File.separator + "sqllite.data")
                  .username("sa");
            break;
         case SQL_SERVER:
            builder.connectionPool()
                  .driverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                  .connectionUrl("jdbc:sqlserver://mssql-2019.msdomain.mw.lab.eng.bos.redhat.com:1433;DatabaseName=dballo04")
                  .username("dballo04")
                  .password("dballo04");
            break;
         case SYBASE:
            builder.connectionPool()
                  .driverClass("com.sybase.jdbc4.jdbc.SybDriver")
                  .connectionUrl("jdbc:sybase:Tds:sybase-160.hosts.mwqe.eng.bos.redhat.com:5000/dballo13")
                  .username("dballo13")
                  .password("dballo13");
            break;
         case H2:
         default:
            UnitTestDatabaseManager.configureUniqueConnectionFactory(builder);
      }
   }

   String binaryType() {
      switch (DB_TYPE) {
         case POSTGRES:
            return "BYTEA";
         case ORACLE:
            return "RAW(255)";
         case SQLITE:
            return "BINARY";
         default:
            return "VARBINARY(255)";
      }
   }

   String dateTimeType() {
      switch (DB_TYPE) {
         case SYBASE:
         case MYSQL:
            return "DATETIME";
         case SQL_SERVER:
            return "DATETIME2";
         case POSTGRES:
         case H2:
         default:
            return "TIMESTAMP";
      }
   }

   protected void createTable(String cacheName, ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> builder) {
      String tableCreation;
      String upperCaseCacheName = cacheName.toUpperCase();
      if (cacheName.equalsIgnoreCase("testPreloadStoredAsBinary")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "picture " + binaryType() + ", " +
               "sex VARCHAR(255), " +
               "birthdate " + dateTimeType() + ", " +
               "PRIMARY KEY (keycolumn))";
      } else if (cacheName.equalsIgnoreCase("testStoreByteArrays")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "keycolumn " + binaryType() + " NOT NULL, " +
               "value1 " + binaryType() + " NOT NULL, " +
               "PRIMARY KEY (keycolumn))";
      } else if (upperCaseCacheName.startsWith("TESTDBHASMOREVALUECOLUMNS")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "picture " + binaryType() + ", " +
               "sex VARCHAR(255), " +
               "birthdate " + dateTimeType() + ", " +
               "value2 VARCHAR(255), " +
               "value3 VARCHAR(255), " +
               "PRIMARY KEY (keycolumn))";
      } else if (upperCaseCacheName.startsWith("TESTDBHASMOREKEYCOLUMNS")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               // The name of the field for the Key schema is "value"
               "value VARCHAR(255) NOT NULL, " +
               "keycolumn2 VARCHAR(255) NOT NULL," +
               "value1 VARCHAR(255) NOT NULL, " +
               "PRIMARY KEY (value, keycolumn2))";
      } else if (upperCaseCacheName.startsWith("TESTDBHASLESSVALUECOLUMNS")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "PRIMARY KEY (keycolumn))";
      } else if (upperCaseCacheName.startsWith("TESTEMBEDDED")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "picture " + binaryType() + ", " +
               "sex VARCHAR(255), " +
               "birthdate " + dateTimeType() + ", " +
               "PRIMARY KEY (name))";
      } else if (upperCaseCacheName.startsWith("TESTENUMFORVALUE")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "NAME VARCHAR(255) NOT NULL, " +
               "sex VARCHAR(255), " +
               "PRIMARY KEY (name))";
      } else if (upperCaseCacheName.startsWith("TESTENUMFORKEY")) {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "sex VARCHAR(255) NOT NULL, " +
               "name VARCHAR(255), " +
               "PRIMARY KEY (sex))";
      } else {
         tableCreation = "CREATE TABLE " + cacheName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "value1 VARCHAR(255) NOT NULL, " +
               "PRIMARY KEY (keycolumn))";
      }

      ConnectionFactoryConfiguration config = builder.create();
      ConnectionFactory factory = ConnectionFactory.getConnectionFactory(config.connectionFactoryClass());
      factory.start(config, getClass().getClassLoader());
      Connection connection = null;
      try {
         connection = factory.getConnection();
         String tableName = tableToSearch(cacheName);
         try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName,
               new String[]{"TABLE"})) {
            if (!rs.next()) {
               try (Statement stmt = connection.createStatement()) {
                  log.debugf("Table: %s doesn't exist, creating via %s%n", tableName, tableCreation);
                  stmt.execute(tableCreation);
               }
            }
         }
      } catch (SQLException t) {
         throw new AssertionError(t);
      } finally {
         factory.releaseConnection(connection);
         factory.stop();
      }
   }

   String tableToSearch(String tableName) {
      if (DB_TYPE == DatabaseType.POSTGRES) return tableName.toLowerCase();
      return tableName.toUpperCase();
   }
}
