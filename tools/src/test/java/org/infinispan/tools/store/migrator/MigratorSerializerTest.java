package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.store.migrator.Element.DATA;
import static org.infinispan.tools.store.migrator.Element.DIALECT;
import static org.infinispan.tools.store.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.store.migrator.Element.ID;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.NAME;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.STRING;
import static org.infinispan.tools.store.migrator.Element.TABLE;
import static org.infinispan.tools.store.migrator.Element.TABLE_NAME_PREFIX;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TIMESTAMP;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.USERNAME;
import static org.infinispan.tools.store.migrator.marshaller.MarshallerType.CURRENT;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * ISPN-7850: A test to ensure that Serializers are loaded correctly and a StackOverflowError does not occur.
 */
@Test(testName = "org.infinispan.tools.store.migrator.MigratorSerializerTest", groups = "functional")
public class MigratorSerializerTest {

   private static final String DB_URL = "jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1";
   private static final String USER = "sa";
   private static final Class DRIVER = org.h2.Driver.class;
   private static final DatabaseType DB_DIALECT = DatabaseType.H2;

   private static final String TABLE_ID_COL = "ID_COLUMN";
   private static final String TABLE_ID_TYPE = "VARCHAR(255)";
   private static final String TABLE_DATA_COL = "DATA_COLUMN";
   private static final String TABLE_DATA_TYPE = "BLOB";
   private static final String TABLE_TS_COL = "TIMESTAMP_COLUMN";
   private static final String TABLE_TS_TYPE = "BIGINT";

   private static final GlobalConfiguration GLOBAL_CONFIG = new GlobalConfigurationBuilder().build();

   private StoreMigrator migrator;

   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      Configuration config = createDatabaseConfig(true);
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(GLOBAL_CONFIG, config);
      Cache<Object, Object> cache = cacheManager.getCache(this.getClass().getName());
      cache.put(1, new TestEntry("1234"));

      Properties props = new Properties();
      createDatabaseConfigProperties(props, true);
      createDatabaseConfigProperties(props, false);
      migrator = new StoreMigrator(props);
   }

   public void testSerializerLoaded() throws Exception {
      migrator.run();
      Configuration config = createDatabaseConfig(false);
      EmbeddedCacheManager cm = new DefaultCacheManager(GLOBAL_CONFIG, config);
      Cache<Object, Object> cache = cm.getCache(this.getClass().getName());
      assertEquals(1, cache.size());
      assertNotNull(cache.get(1));
   }

   private Configuration createDatabaseConfig(boolean source) {
      String tableName = source ? SOURCE.toString() : TARGET.toString();
      return new ConfigurationBuilder().persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .dialect(DB_DIALECT)
            .table()
            .createOnStart(source)
            .tableNamePrefix(tableName)
            .idColumnName(TABLE_ID_COL)
            .idColumnType(TABLE_ID_TYPE)
            .dataColumnName(TABLE_DATA_COL)
            .dataColumnType(TABLE_DATA_TYPE)
            .timestampColumnName(TABLE_TS_COL)
            .timestampColumnType(TABLE_TS_TYPE)
            .connectionPool()
            .connectionUrl(DB_URL)
            .username(USER)
            .driverClass(DRIVER)
            .build();
   }

   private void createDatabaseConfigProperties(Properties props, boolean source) {
      Element type = source ? SOURCE : TARGET;
      props.put(propKey(type, TYPE), STRING.toString());
      props.put(propKey(type, CACHE_NAME), this.getClass().getName());
      props.put(propKey(type, MARSHALLER), CURRENT);
      props.put(propKey(type, DIALECT), DB_DIALECT.toString());

      props.put(propKey(type, CONNECTION_POOL, USERNAME), USER);
      props.put(propKey(type, CONNECTION_POOL, CONNECTION_URL), DB_URL);
      props.put(propKey(type, CONNECTION_POOL, DRIVER_CLASS), DRIVER.getName());

      props.put(propKey(type, TABLE, STRING, TABLE_NAME_PREFIX), type.toString());
      props.put(propKey(type, TABLE, STRING, ID, NAME), TABLE_ID_COL);
      props.put(propKey(type, TABLE, STRING, ID, TYPE), TABLE_ID_TYPE);
      props.put(propKey(type, TABLE, STRING, DATA, NAME), TABLE_DATA_COL);
      props.put(propKey(type, TABLE, STRING, DATA, TYPE), TABLE_DATA_TYPE);
      props.put(propKey(type, TABLE, STRING, TIMESTAMP, NAME), TABLE_TS_COL);
      props.put(propKey(type, TABLE, STRING, TIMESTAMP, TYPE), TABLE_TS_TYPE);
   }

   private String propKey(Element... elements) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return sb.toString();
   }

   @SerializeWith(TestEntrySerializer.class)
   public static class TestEntry implements Serializable {

      private final String s;

      public TestEntry(String s) {
         this.s = s;
      }

      public static TestEntry of(String s) {
         return new TestEntry(s);
      }

      @Override
      public String toString() {
         return s;
      }
   }

   public static class TestEntrySerializer implements AdvancedExternalizer<TestEntry> {

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends TestEntry>> getTypeClasses() {
         return Util.asSet(TestEntry.class);

      }

      @Override
      public Integer getId() {
         return 2017;
      }

      @Override
      public void writeObject(ObjectOutput output, TestEntry object) throws IOException {
         output.writeObject(object.toString());
      }

      @Override
      public TestEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return TestEntry.of((String)input.readObject());
      }
   }
}
