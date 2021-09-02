package org.infinispan.persistence.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.AbstractPersistenceCompatibilityTest;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.data.Value;
import org.testng.annotations.Test;

/**
 * Tests if {@link RocksDBStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.rocksdb.RocksDBStoreCompatibilityTest")
public class RocksDBStoreCompatibilityTest extends AbstractPersistenceCompatibilityTest<Value> {

   private static final Map<Version, String> data = new HashMap<>(2);
   static {
      data.put(Version._10_1, "10_1_x_rocksdb_data");
      data.put(Version._11_0, "11_0_x_rocksdb_data");
   }

   public RocksDBStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   // The rocksdb store should still be able to migrate data from 10.x stream
   @Test
   public void testReadWriteFrom101() throws Exception {
      setParameters(Version._10_1, false, false);

      doTestReadWrite();
   }

   @Test
   public void testReadWriteFrom11() throws Exception {
      setParameters(Version._11_0, false, false);

      doTestReadWrite();
   }

   protected void beforeStartCache() throws Exception {
      String root = data.get(oldVersion);
      copyFiles(root, "data");
      copyFiles(root, "expired");
   }

   private void copyFiles(String root, String qualifier) throws IOException {
      Path destLocation = getStoreLocation(combinePath(tmpDirectory, qualifier), qualifier);
      Files.createDirectories(destLocation);
      String resource = root + "/" + qualifier;
      File[] files = new File(Thread.currentThread().getContextClassLoader().getResource(resource).getPath()).listFiles();
      for (File f : files) {
         String filename = f.getName();
         String destFile = filename;
         if (filename.endsWith("_log")) {
            destFile = filename.replace("_log", ".log");
         }
         copyFile(combinePath(resource, filename), destLocation, destFile);
      }
   }

   @Override
   protected String cacheName() {
      return "rocksdb-cache-store";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder, boolean generatingData) {
      builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class)
            .segmented(oldSegmented)
            .location(combinePath(tmpDirectory, "data"))
            .expiredLocation(combinePath(tmpDirectory, "expired"));
   }
}
