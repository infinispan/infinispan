package org.infinispan.persistence;

import static org.infinispan.persistence.PersistenceUtil.getQualifiedLocation;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

/**
 * Base compatibility test for cache stores.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional")
public abstract class PersistenceCompatibilityTest<T> extends SingleCacheManagerTest {

   private static final int NUMBER_KEYS = 10;
   private final KeyValueWrapper<String, String, T> valueWrapper;
   protected String tmpDirectory;

   protected PersistenceCompatibilityTest(KeyValueWrapper<String, String, T> valueWrapper) {
      this.valueWrapper = valueWrapper;
   }

   private static String key(int index) {
      return "key-" + index;
   }

   private static String value(int index) {
      return "value-" + index;
   }

   protected static void copyFile(String file_10_1, Path location, String fileName) throws IOException {
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(file_10_1, Thread.currentThread().getContextClassLoader());
      File f = new File(location.toFile(), fileName);
      Files.copy(is, f.toPath(), StandardCopyOption.REPLACE_EXISTING);
   }

   @Test
   public void testReadWriteFrom101() throws Exception {
      // 10 keys
      // even keys stored, odd keys removed
      beforeStartCache();
      Cache<String, T> cache = cacheManager.getCache(cacheName());

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         String key = key(i);
         if (i % 2 != 0) {
            assertNull("Expected null value for key " + key, cache.get(key));
         } else {
            assertEquals("Wrong value read for key " + key, value(i), valueWrapper.unwrap(cache.get(key)));
         }
      }

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         if (i % 2 != 0) {
            String key = key(i);
            cache.put(key, valueWrapper.wrap(key, value(i)));
         }
      }

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         String key = key(i);
         assertEquals("Wrong value read for key " + key, value(i), valueWrapper.unwrap(cache.get(key)));
      }

   }

   @Override
   protected void setup() throws Exception {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
      log.debugf("Using tmpDirectory=%s", tmpDirectory);
      super.setup();
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(tmpDirectory);
   }

   protected abstract void beforeStartCache() throws Exception;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().nonClusteredDefault();
      builder.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory());
      amendGlobalConfigurationBuilder(builder);
      DefaultCacheManager cacheManager = new DefaultCacheManager(builder.build());
      cacheManager.defineConfiguration(cacheName(), cacheConfiguration().build());
      return cacheManager;
   }

   protected void amendGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      //no-op by default. To set SerializationContextInitializer (for example)
   }

   protected abstract String cacheName();

   protected abstract void configurePersistence(ConfigurationBuilder builder);

   protected String combinePath(String path, String more) {
      return Paths.get(path, more).toString();
   }

   protected Path getStoreLocation(String location, String qualifier) {
      return getQualifiedLocation(cacheManager.getCacheManagerConfiguration(), location, cacheName(), qualifier);
   }

   private ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL);
      builder.clustering().hash().numSegments(4);
      configurePersistence(builder);
      return builder;
   }

}
