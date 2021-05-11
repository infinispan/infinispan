package org.infinispan.server.persistence;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * @author William Burns
 * @author Ryan Emerson
 * @since 12.1
 */
public class EventLoggerCompatibilityTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   String tmpDirectory;

   @Before
   public void startup() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
   }

   @After
   public void teardown() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Test
   public void testReadWriteFrom11EventCache() throws Exception {
      String cacheName = "event-cache";
      String file = "sfs/11_0/event_log_cache.dat";
      InputStream is = FileLookupFactory.newInstance()
            .lookupFile(file, Thread.currentThread().getContextClassLoader());
      File sfsFile = SingleFileStore
            .getStoreFile(new GlobalConfigurationBuilder().build(), tmpDirectory, cacheName);
      if (!sfsFile.exists()) {
         sfsFile.getParentFile().mkdirs();
      }

      //copy data to the store file
      Files.copy(is, sfsFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      Configuration config = new ConfigurationBuilder()
            .persistence().addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory)
            .build();

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.createCache(cacheName, config);
            assertEquals(43, cache.size());
         }
      });
   }
}
