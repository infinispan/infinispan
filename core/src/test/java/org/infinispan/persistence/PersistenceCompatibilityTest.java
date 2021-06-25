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
import java.util.concurrent.CompletionException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Value;
import org.testng.annotations.Test;

/**
 * Base compatibility test for cache stores.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional")
public abstract class PersistenceCompatibilityTest<T> extends SingleCacheManagerTest {

   protected enum Version {
      _10_1,
      _11_0
   }

   protected static final int NUMBER_KEYS = 10;
   protected final KeyValueWrapper<String, Value, T> valueWrapper;
   protected String tmpDirectory;

   protected PersistenceCompatibilityTest(KeyValueWrapper<String, Value, T> valueWrapper) {
      this.valueWrapper = valueWrapper;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected String key(int index) {
      return "key-" + index;
   }

   protected Value value(int index) {
      String i = Integer.toString(index);
      return new Value(i, i);
   }

   protected static void copyFile(String src, Path dst, String fileName) throws IOException {
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(src, Thread.currentThread().getContextClassLoader());
      File f = new File(dst.toFile(), fileName);
      Files.copy(is, f.toPath(), StandardCopyOption.REPLACE_EXISTING);
   }

   @Test
   public void testReadWriteFrom101() throws Exception {
      beforeStartCache(Version._10_1);
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class, ".*ISPN000616.*",
            () -> cacheManager.getCache(cacheName()));
   }

   protected void doTestReadWriteFrom101() throws Exception {
      beforeStartCache(Version._10_1);
      Cache<String, String> cache = cacheManager.getCache(cacheName());

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         String key = key(i);
         if (i % 2 != 0) {
            assertNull("Expected null value for key " + key, cache.get(key));
         } else {
            assertEquals("Wrong value read for key " + key, "value-" + i, cache.get(key));
         }
      }

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         if (i % 2 != 0) {
            String key = key(i);
            cache.put(key, "value-" + i);
         }
      }

      for (int i = 0; i < NUMBER_KEYS; ++i) {
         String key = key(i);
         assertEquals("Wrong value read for key " + key, "value-" + i, cache.get(key));
      }
   }

   @Test
   public void testReadWriteFrom11() throws Exception {
      // 10 keys
      // even keys stored, odd keys removed
      beforeStartCache(Version._11_0);
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

      // Restart the CacheManager to ensure that the entries are still readable on restart
      cacheManager.stop();

      try (EmbeddedCacheManager cm = createCacheManager()) {
         cache = cm.getCache(cacheName());
         for (int i = 0; i < NUMBER_KEYS; ++i) {
            String key = key(i);
            assertEquals("Wrong value read for key " + key, value(i), valueWrapper.unwrap(cache.get(key)));
         }
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

   protected abstract void beforeStartCache(Version version) throws Exception;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().nonClusteredDefault();
      builder.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory());
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
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
