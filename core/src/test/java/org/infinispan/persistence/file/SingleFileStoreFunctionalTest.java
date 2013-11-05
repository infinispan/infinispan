package org.infinispan.persistence.file;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.*;

/**
 * Single file cache store functional test.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileStoreFunctionalTest")
public class SingleFileStoreFunctionalTest extends BaseStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence
         .addSingleFileStore()
         .location(tmpDirectory)
         .preload(preload);
      return persistence;
   }

   public void testParsingEmptyElement() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<persistence passivation=\"false\"> \n" +
            "<singleFile shared=\"false\" preload=\"true\"/> \n" +
            "</persistence>\n" +
            "</default>\n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals("v1", cache.get(1));
            SingleFileStore cacheLoader = (SingleFileStore) TestingUtil.getFirstLoader(cache);
            assertEquals("Infinispan-SingleFileStore", cacheLoader.getConfiguration().location());
            assertEquals(-1, cacheLoader.getConfiguration().maxEntries());
         }
      });
   }

   public void testParsingElement() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<eviction maxEntries=\"100\"/>" +
            "<persistence passivation=\"false\"> \n" +
            "<singleFile maxEntries=\"100\" shared=\"false\" preload=\"true\" location=\"other-location\"/> \n" +
            "</persistence>\n" +
            "</default>\n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals("v1", cache.get(1));
            SingleFileStore store = (SingleFileStore) TestingUtil.getFirstLoader(cache);
            assertEquals("other-location", store.getConfiguration().location());
            assertEquals(100, store.getConfiguration().maxEntries());
         }
      });
   }

}
