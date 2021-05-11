package org.infinispan.persistence.file;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.test.data.Value;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests if {@link SingleFileStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.file.SingleFileStoreCompatibilityTest")
public class SingleFileStoreCompatibilityTest extends PersistenceCompatibilityTest<Value> {

   private static final String EVENT_LOG_11_0 = Version._11_0.toString() + "_event";

   private static final Map<String, String> data = new HashMap<>(2);
   static {
      data.put(Version._10_1.toString(), "sfs/10_1/sfs-store-cache.dat");
      data.put(Version._11_0.toString(), "sfs/11_0/sfs-store-cache.dat");
      data.put(EVENT_LOG_11_0, "sfs/11_0/event_log_cache.dat");
   }

   public SingleFileStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void beforeStartCache(Version version) throws Exception {
      beforeStartCache(version.toString());
   }

   private void beforeStartCache(String version) throws Exception {
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(data.get(version), Thread.currentThread().getContextClassLoader());
      File sfsFile = SingleFileStore
            .getStoreFile(cacheManager.getCacheManagerConfiguration(), tmpDirectory, cacheName());
      if (!sfsFile.exists()) {
         //create parent directories
         sfsFile.getParentFile().mkdirs();
      }

      //copy data to the store file
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

   public void testReadWriteFrom11EventCache() throws Exception {
      // 10 keys
      // even keys stored, odd keys removed
      beforeStartCache(EVENT_LOG_11_0);

      Cache<Object, Object> cache = cacheManager.getCache(cacheName());

      assertEquals(10, cache.size());
   }
}
