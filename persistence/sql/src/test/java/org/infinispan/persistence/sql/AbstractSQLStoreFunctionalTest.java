package org.infinispan.persistence.sql;

import static org.infinispan.persistence.jdbc.common.DatabaseType.H2;
import static org.infinispan.persistence.jdbc.common.DatabaseType.SQLITE;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.sql.configuration.AbstractSchemaJdbcConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Numerics;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Sex;
import org.infinispan.transaction.TransactionMode;
import org.mockito.Mockito;
import org.postgresql.Driver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;
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
      return new Person(name, new Address());
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
   protected void assertPersonEqual(Person firstPerson, Person secondPerson) {
      switch (DB_TYPE) {
         // These databases right pad CHAR to use up the entire space
         case H2:
         case DB2:
         case ORACLE:
         case SQL_SERVER:
         case POSTGRES:
            if (!firstPerson.equalsIgnoreWhitespaceAddress(secondPerson)) {
               fail("expected:<" + firstPerson + "> but was:<" + secondPerson + ">");
            }
            break;
         default:
            super.assertPersonEqual(firstPerson, secondPerson);
      }

   }

   @Override
   public void testPreloadStoredAsBinary() {
      schemaConsumer = builder ->
            builder.schema()
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

   @Test(expectedExceptions=CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*ISPN000651.*")
   public void testMaxIdleNotAllowedWithoutPassivation() {
      String cacheName = "testMaxIdleNotAllowedWithoutPassivation";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      cb.expiration().maxIdle(1);
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());
      // will start the cache
      cacheManager.getCache(cacheName);
   }

   public void testRollback() throws SystemException, NotSupportedException {
      if (!transactionalCache) {
         return;
      }
      String cacheName = "testRollback";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

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

   public void testKeyWithNullFields(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(true)
                  .keyMessageName("Address")
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");

      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      String cacheName = m.getName();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      Address key1 = new Address(null, "Newcastle", 1);
      Address key2 = new Address("", "Newcastle", 2);
      Person value1 = new Person("Alan", key1);
      Person value2 = new Person("Bobby", key2);
      assertNull(cache.get(key1));
      cache.put(key1, value1);
      cache.put(key2, value2);
      assertEquals(value1, cache.get(key1));
      cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).clear();
      assertTrue(cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).isEmpty());
      assertEquals(value1, cache.get(key1));
      assertEquals(value2, cache.get(key2));
   }

   public void testDBHasMoreKeyColumnsWithKeySchema(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(false)
                  .keyMessageName("Key")
                  .packageName("org.infinispan.test.core");
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            ".*Primary key (?i)(KEYCOLUMN2) was not found.*",
            () -> testSimpleGetAndPut(m.getName(), new Key("mykey"), "value1"));
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
            builder.schema()
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
            builder.schema()
                  .embeddedKey(false)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "key", new Person("joe"));
   }

   public void testEmbeddedKey(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(true)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "joe", new Person("joe"));
   }

   public void testEnumForKey(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(false)
                  .keyMessageName("Sex")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), Sex.FEMALE, "samantha");
   }

   public void testEnumForValue(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(false)
                  .messageName("Sex")
                  .packageName("org.infinispan.test.core");
      testSimpleGetAndPut(m.getName(), "samantha", Sex.FEMALE);
   }

   public void testEmbeddedLoadSchemaAfterCreation(Method m) {
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(true)
                  .messageName("Person")
                  .packageName("org.infinispan.test.core");

      final AtomicBoolean loadSchema = new AtomicBoolean(false);
      SerializationContextRegistry scr = TestingUtil.extractGlobalComponent(cacheManager, SerializationContextRegistry.class);
      SerializationContextRegistry spyScr = Mockito.spy(scr);
      Mockito.when(spyScr.getUserCtx()).then(ivk -> {
         if (loadSchema.get()) return scr.getUserCtx();
         return ProtobufUtil.newSerializationContext();
      });
      TestingUtil.replaceComponent(cacheManager, SerializationContextRegistry.class, spyScr, true);

      String cacheName = m.getName();
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      // This should fail because the schema does not exist.
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class,
            CacheConfigurationException.class,
            "ISPN008047: Schema not found for : org.infinispan.test.core.Person",
            () -> cacheManager.getCache(cacheName)
      );

      loadSchema.set(true);
      // Schema registered successfully afterwards.
      // Should be fully functional now.
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      String key = "joe";
      Person value = new Person("joe");
      assertNull(cache.get(key));
      cache.put(key, value);
      assertEquals(value, cache.get(key));
   }

   public void testNumericColumns(Method m) {
      long key = Integer.MAX_VALUE;
      String cacheName = m.getName();
      schemaConsumer = builder ->
            builder.schema()
                  .embeddedKey(true)
                  .messageName("Numerics")
                  .packageName("org.infinispan.test.core");
      Numerics v = new Numerics(Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, 9_000_000_000L);

      // This test might operate in memory.
      testSimpleGetAndPut(cacheName, key, v);

      // Shutdown and start cache to retrieve data from store.
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      cache.shutdown();
      cache.start();

      // Check again.
      assertEquals(v, cache.get(key));
   }

   private void testSimpleGetAndPut(String cacheName, Object key, Object value) {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<Object, Object> cache = cacheManager.getCache(cacheName);

      assertNull(cache.get(key));

      cache.put(key, value);

      assertEquals(value, cache.get(key));

      List<Map.Entry<Object, Object>> entryList = cache.entrySet().stream().collect(Collectors.toList());
      assertEquals(1, entryList.size());

      Map.Entry<Object, Object> entry = entryList.get(0);
      assertEquals(key, entry.getKey());
      assertEquals(value, entry.getValue());
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

   String integerType() {
      switch (DB_TYPE) {
         case SQLITE:
            return "INTEGER";
         default:
            return "NUMERIC(10, 0)";
      }
   }

   String longType() {
      switch (DB_TYPE) {
         case SQLITE:
            return "INTEGER";
         default:
            return "NUMERIC(19, 0)";
      }
   }

   String floatType() {
      switch (DB_TYPE) {
         case SQLITE:
            return "REAL";
         default:
            return "NUMERIC(45, 6)";
      }
   }

   String doubleType() {
      switch (DB_TYPE) {
         case SQLITE:
            return "REAL";
         default:
            return "DOUBLE";
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

   String booleanType() {
      switch (DB_TYPE) {
         case SQL_SERVER:
            return "BIT";
         case ORACLE:
         case ORACLE_XE:
            return "NUMBER(1, 0)";
         default:
            return "BOOLEAN";
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
      if ("testKeyWithNullFields".equalsIgnoreCase(cacheName)) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "NAME VARCHAR(255) NOT NULL, " +
               "street VARCHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "PRIMARY KEY (zip))";
      } else if ("testPreloadStoredAsBinary".equalsIgnoreCase(cacheName)) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "keycolumn VARCHAR(255) NOT NULL, " +
               "NAME VARCHAR(255) NOT NULL, " +
               "street CHAR(255), " +
               "city VARCHAR(255), " +
               "zip INT, " +
               "picture " + binaryType() + ", " +
               "accepted_tos " + booleanType() + ", " +
               "sex VARCHAR(255), " +
               "birthdate " + dateTimeType() + ", " +
               "moneyOwned NUMERIC(10, 4), " +
               "moneyOwed FLOAT, " +
               "decimalField DECIMAL(10, 4), " +
               "realField REAL, " +
               "PRIMARY KEY (keycolumn))";
      } else if ("testStoreByteArrays".equalsIgnoreCase(cacheName)) {
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
               // The name of the field for the Key schema is "value1"
               "value1 VARCHAR(255) NOT NULL, " +
               "keycolumn2 VARCHAR(255) NOT NULL, " +
               "value2 VARCHAR(255) NOT NULL, " +
               "PRIMARY KEY (value1, keycolumn2))";
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
      } else if ("TESTNUMERICCOLUMNS".equals(upperCaseCacheName)) {
         tableCreation = "CREATE TABLE " + tableName + " (" +
               "keycolumn " + integerType() + ", " +
               "simpleLong " + longType() + ", " +
               "simpleFloat " + floatType() + ", " +
               "simpleDouble " + doubleType() + ", " +
               "largeInteger " + integerType() + ", " +
               "PRIMARY KEY (keycolumn))";
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
