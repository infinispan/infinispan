package org.infinispan.persistence.rocksdb;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests if {@link RocksDBStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.rocksdb.RocksDBStoreCompatibilityTest")
public class RocksDBStoreCompatibilityTest extends PersistenceCompatibilityTest<String> {

   private static final String DATA_10_1_FOLDER = Paths.get("10_1_x_rocksdb_data", "data").toString();
   private static final String EXPIRED_10_1_FOLDER = Paths.get("10_1_x_rocksdb_data", "expired").toString();
   private static final String[] DATA_10_1_FILES = new String[]{
         "000003_log",
         "CURRENT",
         "IDENTITY",
         "LOCK",
         "LOG",
         "MANIFEST-000001",
         "OPTIONS-000005"
   };

   public RocksDBStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void beforeStartCache() throws Exception {
      Path dataLocation = getStoreLocation(combinePath(tmpDirectory, "data"), "data");
      Path expiredLocation = getStoreLocation(combinePath(tmpDirectory, "expired"), "expired");

      dataLocation.toFile().mkdirs();
      expiredLocation.toFile().mkdirs();

      for (String filename : DATA_10_1_FILES) {
         String dstFile = filename;
         if (filename.endsWith("_log")) {
            dstFile = filename.replace("_log", ".log");
         }
         copyFile(combinePath(DATA_10_1_FOLDER, filename), dataLocation, dstFile);
         copyFile(combinePath(EXPIRED_10_1_FOLDER, filename), expiredLocation, dstFile);
      }
   }

   @Override
   protected String cacheName() {
      return "rocksdb-cache-store";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .segmented(false)
            .location(combinePath(tmpDirectory, "data"))
            .expiredLocation(combinePath(tmpDirectory, "expired"));
   }
}
