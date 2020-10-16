package org.infinispan.persistence.jpa;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
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
import org.infinispan.persistence.KeyValueWrapper;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

/**
 * Tests if {@link JpaStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jpa.JpaStoreCompatibilityTest")
public class JpaStoreCompatibilityTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "jps-store-cache";
   private static final String DB_FILE_NAME = "jpa_db.mv.db";
   private static final String DATA_10_1_FOLDER = "10_1_x_jpa_data";
   private static final int NUMBER_KEYS = 10;

   private final KeyValueWrapper<String, String, KeyValueEntity> valueWrapper = JpaKeyValueWrapper.INSTANCE;

   private String tmpDirectory;

   @Test
   public void testReadWriteFrom101() throws Exception {
      new File(tmpDirectory).mkdirs();
      String src = Paths.get(DATA_10_1_FOLDER, DB_FILE_NAME).toString();
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(src, Thread.currentThread().getContextClassLoader());
      File f = new File(Paths.get(tmpDirectory).toFile(), DB_FILE_NAME);
      Files.copy(is, f.toPath(), StandardCopyOption.REPLACE_EXISTING);

      Cache<String, KeyValueEntity> cache = cacheManager.getCache(CACHE_NAME);

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

   private String key(int index) {
      return "key-" + index;
   }

   private static String value(int index) {
      return "value-" + index;
   }

   @Override
   protected void setup() throws Exception {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
      log.debugf("Using tmpDirectory=%s", tmpDirectory);
      super.setup();
   }

   @Override
   protected void teardown() {
      super.teardown();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL);
      builder.clustering().hash().numSegments(4);
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .entityClass(KeyValueEntity.class)
            .storeMetadata(true)
            .persistenceUnitName("org.infinispan.persistence.jpa.compatibility_test")
            .segmented(false);

      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory());
      globalBuilder.serialization().addContextInitializer(JpaSCI.INSTANCE);
      DefaultCacheManager cacheManager = new DefaultCacheManager(globalBuilder.build());
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());

      return cacheManager;
   }
}
