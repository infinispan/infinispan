package org.infinispan.persistence;

import static org.infinispan.persistence.PersistenceUtil.getQualifiedLocation;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Value;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Base compatibility test for cache stores.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public abstract class AbstractPersistenceCompatibilityTest<T> extends SingleCacheManagerTest {

   protected enum Version {
      _10_1,
      _11_0,
      _12_0,
      _12_1,
   }

   protected static final int NUMBER_KEYS = 10;
   protected final KeyValueWrapper<String, Value, T> valueWrapper;
   protected String tmpDirectory;
   protected Version oldVersion;
   protected boolean oldSegmented;
   protected boolean newSegmented;

   protected AbstractPersistenceCompatibilityTest(KeyValueWrapper<String, Value, T> valueWrapper) {
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

   protected abstract void beforeStartCache() throws Exception;

   protected void setParameters(Version oldVersion, boolean oldSegmented, boolean newSegmented) {
      this.oldVersion = oldVersion;
      this.oldSegmented = oldSegmented;
      this.newSegmented = newSegmented;
   }

   protected void doTestReadWrite() throws Exception {
      // 10 keys
      // even keys stored, odd keys removed
      beforeStartCache();

      cacheManager.defineConfiguration(cacheName(), cacheConfiguration(true).build());
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
         cm.defineConfiguration(cacheName(), cacheConfiguration(false).build());
         cache = cm.getCache(cacheName());
         for (int i = 0; i < NUMBER_KEYS; ++i) {
            String key = key(i);
            assertEquals("Wrong value read for key " + key, value(i), valueWrapper.unwrap(cache.get(key)));
         }
      }
   }

   @Test(enabled = false)
   public void generateOldData() throws IOException {
      newSegmented = false;
      ConfigurationBuilder cacheBuilder = cacheConfiguration(true);
      cacheManager.defineConfiguration(cacheName(), cacheBuilder.build());
      Cache<String, T> cache = cacheManager.getCache(cacheName());

      // 10 keys
      // even keys stored, odd keys removed
      for (int i = 0; i < NUMBER_KEYS; ++i) {
         if (i % 2 == 0) {
            String key = key(i);
            cache.put(key, valueWrapper.wrap(key, value(i)));
         }
      }
      cache.stop();

      Path sourcePath = Paths.get(tmpDirectory);
      Path targetPath = Paths.get("generated").toAbsolutePath();
      Files.createDirectories(targetPath);
      Files.walkFileTree(sourcePath, new FileVisitor<Path>() {
         @Override
         public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)));
            return FileVisitResult.CONTINUE;
         }

         @Override
         public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.copy(file, targetPath.resolve(sourcePath.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
         }

         @Override
         public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
         }

         @Override
         public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
         }
      });
      log.infof("Data generated in %s", targetPath);
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

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().nonClusteredDefault();
      builder.globalState().persistentLocation(tmpDirectory);
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      amendGlobalConfigurationBuilder(builder);
      return TestCacheManagerFactory.createCacheManager(builder, null);
   }

   protected void amendGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      //no-op by default. To set SerializationContextInitializer (for example)
   }

   protected abstract String cacheName();

   protected abstract void configurePersistence(ConfigurationBuilder builder, boolean generatingData);

   protected String combinePath(String path, String more) {
      return Paths.get(path, more).toString();
   }

   protected Path getStoreLocation(String location, String qualifier) {
      return getQualifiedLocation(cacheManager.getCacheManagerConfiguration(), location, cacheName(), qualifier);
   }

   protected ConfigurationBuilder cacheConfiguration(boolean generatingData) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL);
      builder.clustering().hash().numSegments(4);
      configurePersistence(builder, generatingData);
      return builder;
   }
}
