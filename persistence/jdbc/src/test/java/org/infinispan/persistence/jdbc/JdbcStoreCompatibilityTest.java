package org.infinispan.persistence.jdbc;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.AbstractPersistenceCompatibilityTest;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.test.data.Value;
import org.testng.annotations.Test;

/**
 * Tests if {@link JdbcStringBasedStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.JdbcStoreCompatibilityTest")
public class JdbcStoreCompatibilityTest extends AbstractPersistenceCompatibilityTest<Value> {

   private static final Map<Version, String> data = new HashMap<>(2);

   static {
      data.put(Version._10_1, "10.1.sql");
      data.put(Version._11_0, "11.0.sql");
   }

   public JdbcStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   // The jdbc store should still be able to migrate data from 10.x stream
   @Test
   public void testReadWriteFrom101() throws Exception {
      oldVersion = Version._10_1;
      doTestReadWrite();
   }

   @Test
   public void testReadWriteFrom11() throws Exception {
      oldVersion = Version._11_0;
      doTestReadWrite();
   }

   @Override
   protected void beforeStartCache() {
      Path path = Paths.get(System.getProperty("build.directory"), "test-classes", data.get(oldVersion));
      // Create the DB
      try (Connection c = DriverManager.getConnection(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM '%s'", Paths.get(System.getProperty("java.io.tmpdir"), oldVersion.toString()), path), "sa", "")) {
         // Nothing to do
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   protected String cacheName() {
      return "jdbc_store_cache";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder, boolean generatingData) {
      JdbcStringBasedStoreConfigurationBuilder jdbcB = builder.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      jdbcB.segmented(oldSegmented);
      jdbcB.table()
            .createOnStart(generatingData)
            .tableNamePrefix("ISPN_STRING_TABLE")
            .idColumnName("ID_COLUMN").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA_COLUMN").dataColumnType("BINARY VARYING")
            .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType("BIGINT")
            .segmented(false);
      jdbcB.connectionPool()
            .driverClass(org.h2.Driver.class)
            //-1 = never closed (== thread leak reported at the end), 0 = close when all connections are closed
            .connectionUrl(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=0", Paths.get(System.getProperty("java.io.tmpdir"), oldVersion.toString())))
            .username("sa");
   }
}
