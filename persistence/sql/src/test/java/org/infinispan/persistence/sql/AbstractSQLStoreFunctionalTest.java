package org.infinispan.persistence.sql;

import static org.infinispan.persistence.jdbc.common.DatabaseType.H2;
import static org.infinispan.persistence.jdbc.common.DatabaseType.SQLITE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
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

import util.JdbcConnection;

public abstract class AbstractSQLStoreFunctionalTest extends BaseStoreFunctionalTest {

   protected final DatabaseType DB_TYPE;
   protected final boolean transactionalCache;
   protected final boolean transactionalStore;

   protected String tmpDirectory;
   protected Consumer<AbstractSchemaJdbcConfigurationBuilder<?, ?>> schemaConsumer;

   protected static final String DATABASE = System.getProperty("org.infinispan.test.sqlstore.database");
   protected static final String JDBC_URL = System.getProperty("org.infinispan.test.sqlstore.jdbc.url");
   protected static final String JDBC_USERNAME = System.getProperty("org.infinispan.test.sqlstore.jdbc.username");
   protected static final String JDBC_PASSWORD = System.getProperty("org.infinispan.test.sqlstore.jdbc.password");
   protected static Map<DatabaseType, JdbcConnection> databasesFromSystemProperty = new HashMap<>();

   static {
      if(DATABASE != null) {
         databasesFromSystemProperty = getDatabases();
      }
   }

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

      PooledConnectionFactoryConfigurationBuilder<?> connectionPool = null;
      if(!(DB_TYPE == SQLITE || DB_TYPE == H2)) {
         connectionPool = addJdbcConnection(builder);
      }

      switch (DB_TYPE) {
         case POSTGRES:
            connectionPool
                  .driverClass(Driver.class);
            break;
         case ORACLE:
            connectionPool
                  .driverClass("oracle.jdbc.OracleDriver");
            break;
         case MARIA_DB:
            connectionPool
                  .driverClass("org.mariadb.jdbc.Driver");
            break;
         case DB2:
            connectionPool
                  .driverClass("com.ibm.db2.jcc.DB2Driver");
            break;
         case MYSQL:
            connectionPool
                  .driverClass("com.mysql.cj.jdbc.Driver");
            break;
         case SQLITE:
            builder.connectionPool()
                  .driverClass("org.sqlite.JDBC")
                  .connectionUrl("jdbc:sqlite:" + tmpDirectory + File.separator + "sqllite.data")
                  .username("sa");
            break;
         case SQL_SERVER:
            connectionPool
                  .driverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            break;
         case SYBASE:
            connectionPool
                  .driverClass("com.sybase.jdbc4.jdbc.SybDriver");
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
         case MARIA_DB:
            return "DATETIME";
         case SQL_SERVER:
            return "DATETIME2";
         case POSTGRES:
         case H2:
         default:
            return "TIMESTAMP";
      }
   }

   protected void createTable(String cacheName, String tableName, ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> builder) {
      String tableCreation;
      String upperCaseCacheName = cacheName.toUpperCase();
      if (cacheName.equalsIgnoreCase("testPreloadStoredAsBinary")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
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
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "keycolumn " + binaryType() + " NOT NULL, " +
               "value1 " + binaryType() + " NOT NULL, " +
               "PRIMARY KEY (keycolumn))";
      } else if (upperCaseCacheName.startsWith("TESTDBHASMOREVALUECOLUMNS")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
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
         tableCreation = "CREATE TABLE " + tableName + " (" +
               // The name of the field for the Key schema is "value"
               "value VARCHAR(255) NOT NULL, " +
               "keycolumn2 VARCHAR(255) NOT NULL," +
               "value1 VARCHAR(255) NOT NULL, " +
               "PRIMARY KEY (value, keycolumn2))";
      } else if (upperCaseCacheName.startsWith("TESTDBHASLESSVALUECOLUMNS")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "PRIMARY KEY (keycolumn))";
      } else if (upperCaseCacheName.startsWith("TESTEMBEDDED")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "picture " + binaryType() + ", " +
               "sex VARCHAR(255), " +
               "birthdate " + dateTimeType() + ", " +
               "PRIMARY KEY (name))";
      } else if (upperCaseCacheName.startsWith("TESTENUMFORVALUE")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "NAME VARCHAR(255) NOT NULL, " +
               "sex VARCHAR(255), " +
               "PRIMARY KEY (name))";
      } else if (upperCaseCacheName.startsWith("TESTENUMFORKEY")) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "sex VARCHAR(255) NOT NULL, " +
               "name VARCHAR(255), " +
               "PRIMARY KEY (sex))";
      } else {
         tableCreation = "CREATE TABLE " + tableName + " (" +
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
         String modifiedTableName = tableToSearch(tableName);
         try (ResultSet rs = connection.getMetaData().getTables(null, null, modifiedTableName,
               new String[]{"TABLE"})) {
            if (!rs.next()) {
               try (Statement stmt = connection.createStatement()) {
                  log.debugf("Table: %s doesn't exist, creating via %s%n", modifiedTableName, tableCreation);
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

   private PooledConnectionFactoryConfigurationBuilder<?> addJdbcConnection(AbstractSchemaJdbcConfigurationBuilder<?, ?> builder) {
      if(JDBC_URL != null && JDBC_PASSWORD != null && JDBC_USERNAME != null) {
         JdbcConnection jdbcConnection = databasesFromSystemProperty.get(DB_TYPE);
         return builder.connectionPool()
                 .connectionUrl(jdbcConnection.getJdbcUrl())
                 .username(jdbcConnection.getUsername())
                 .password(jdbcConnection.getPassword());
      }
      throw new IllegalArgumentException("JDBC connection wasn't provided through System Properties");
   }

   protected static HashMap<DatabaseType, JdbcConnection> getDatabases() {
      Objects.requireNonNull(JDBC_URL);
      Objects.requireNonNull(JDBC_USERNAME);
      Objects.requireNonNull(JDBC_PASSWORD);
      Objects.requireNonNull(DATABASE);
      List<DatabaseType> databaseTypes = Arrays.stream(DATABASE.split(",")).map(DatabaseType::guessDialect).collect(Collectors.toList());
      HashMap<DatabaseType, JdbcConnection> map = new HashMap<>();
      for (int i = 0; i < databaseTypes.size(); i++) {
         String jdbcURL = JDBC_URL.split(",")[i];
         String username = JDBC_USERNAME.split(",")[i];
         String password = JDBC_PASSWORD.split(",")[i];

         JdbcConnection jdbcConnection = new JdbcConnection(jdbcURL, username, password);
         DatabaseType databaseType = databaseTypes.get(i);
         map.put(databaseType, jdbcConnection);
      }
      return map;
   }

}
