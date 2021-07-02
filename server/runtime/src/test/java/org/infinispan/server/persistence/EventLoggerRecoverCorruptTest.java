package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test that replicates 11.x -> 12.1.x corrupt migration for the ServerEventLogger.
 *
 * @author Ryan Emerson
 * @since 13.0
 */
public class EventLoggerRecoverCorruptTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   static final String CACHE_NAME = "update-cache";

   private static String tmpDirectory;

   @BeforeClass
   public static void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(EventLoggerRecoverCorruptTest.class);
      new File(tmpDirectory).mkdirs();
   }

   @AfterClass
   public static void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Test
   public void testAllEntriesRecovered() throws Exception {
      InputStream is = FileLookupFactory.newInstance().lookupFile("sfs/corrupt/___event_log_cache.dat", Thread.currentThread().getContextClassLoader());
      Files.copy(is, Paths.get(tmpDirectory).resolve(CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);

      Configuration config = new ConfigurationBuilder()
            .persistence()
            .addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory)
            .build();

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.createCache(CACHE_NAME, config);
            assertEquals(37, cache.size());
         }
      });
   }
}
