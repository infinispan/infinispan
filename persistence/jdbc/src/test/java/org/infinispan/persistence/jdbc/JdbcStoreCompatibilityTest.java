package org.infinispan.persistence.jdbc;

import java.io.File;
import java.nio.file.Paths;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.testng.annotations.Test;

/**
 * Tests if {@link JdbcStringBasedStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.JdbcStoreCompatibilityTest")
public class JdbcStoreCompatibilityTest extends PersistenceCompatibilityTest<String> {

   private static final String DB_FILE_NAME = "jdbc_db.mv.db";
   private static final String DATA_10_1_FOLDER = "10_1_x_jdbc_data";

   public JdbcStoreCompatibilityTest() {
      super(new KeyValueWrapper<String, String, String>() {
         @Override
         public String wrap(String key, String value) {
            return value;
         }

         @Override
         public String unwrap(String value) {
            return value;
         }
      });
   }

   @Override
   protected void beforeStartCache() throws Exception {
      new File(tmpDirectory).mkdirs();
      copyFile(combinePath(DATA_10_1_FOLDER, DB_FILE_NAME), Paths.get(tmpDirectory), DB_FILE_NAME);
   }

   @Override
   protected String cacheName() {
      return "jdbc_store_cache";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      JdbcStringBasedStoreConfigurationBuilder jdbcB = builder.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      jdbcB.table()
            .createOnStart(true)
            .tableNamePrefix("ISPN_STRING_TABLE")
            .idColumnName("ID_COLUMN").idColumnType("VARCHAR(255)")
            .dataColumnName("DATA_COLUMN").dataColumnType("BINARY")
            .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType("BIGINT")
            .segmented(false);
      jdbcB.connectionPool()
            .driverClass(org.h2.Driver.class)
            //-1 = never closed (== thread leak reported at the end), 0 = close when all connection are closed
            .connectionUrl(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=0", combinePath(tmpDirectory, "jdbc_db")))
            .username("sa");
   }
}
