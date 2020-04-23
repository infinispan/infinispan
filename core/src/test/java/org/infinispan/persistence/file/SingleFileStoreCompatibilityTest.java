package org.infinispan.persistence.file;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.testng.annotations.Test;

/**
 * Tests if {@link SingleFileStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.file.SingleFileStoreCompatibilityTest")
public class SingleFileStoreCompatibilityTest extends PersistenceCompatibilityTest<String> {

   private static final String DATA_10_1 = "10_1_x_sfs_data/sfs-store-cache.dat";

   public SingleFileStoreCompatibilityTest() {
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
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(DATA_10_1, Thread.currentThread().getContextClassLoader());
      File sfsFile = SingleFileStore
            .getStoreFile(cacheManager.getCacheManagerConfiguration(), tmpDirectory, cacheName());
      if (!sfsFile.exists()) {
         //create parent directories
         sfsFile.getParentFile().mkdirs();
      }

      //copy 10.1 data to the store file
      Files.copy(is, sfsFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
   }

   @Override
   protected String cacheName() {
      return "sfs-cache-store";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory);
   }

}
