package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileCacheStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileCacheStoreConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Single file cache store functional test.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "loaders.file.SingleFileCacheStoreFunctionalTest")
public class SingleFileCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      loaders
         .addSingleFileCacheStore()
         .location(tmpDirectory)
         .purgeSynchronously(true);
      return loaders;
   }

   public void testParsingEmptyElement() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<loaders passivation=\"false\" shared=\"false\" preload=\"true\"> \n" +
            "<singleFileStore/> \n" +
            "</loaders>\n" +
            "</default>\n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals("v1", cache.get(1));
            CacheStore store = extractComponent(cache, CacheLoaderManager.class).getCacheStore();
            assertTrue(store instanceof SingleFileCacheStore);
            SingleFileCacheStoreConfiguration storeConfiguration = null;
            if (store.getConfiguration() instanceof SingleFileCacheStoreConfiguration) {
               storeConfiguration = (SingleFileCacheStoreConfiguration) store.getConfiguration();
            } else {
               fail("The Configuration bean for a SingleFileCacheStore has to be an instance of " +
                     "SingleFileCacheStoreConfiguration");
            }
            assertEquals("Infinispan-SingleFileCacheStore", storeConfiguration.location());
            assertEquals(-1, storeConfiguration.maxEntries());
         }
      });
   }

   public void testParsingElement() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA +
            "<default>\n" +
            "<eviction maxEntries=\"100\"/>" +
            "<loaders passivation=\"false\" shared=\"false\" preload=\"true\"> \n" +
            "<singleFileStore maxEntries=\"100\" location=\"other-location\"/> \n" +
            "</loaders>\n" +
            "</default>\n" + INFINISPAN_END_TAG;
      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            cache.put(1, "v1");
            assertEquals("v1", cache.get(1));
            CacheStore store = extractComponent(cache, CacheLoaderManager.class).getCacheStore();
            assertTrue(store instanceof SingleFileCacheStore);
            SingleFileCacheStoreConfiguration storeConfiguration = null;
            if (store.getConfiguration() instanceof SingleFileCacheStoreConfiguration) {
               storeConfiguration = (SingleFileCacheStoreConfiguration) store.getConfiguration();
            } else {
               fail("The Configuration bean for a SingleFileCacheStore has to be an instance of " +
                     "SingleFileCacheStoreConfiguration");
            }
            assertEquals("other-location", storeConfiguration.location());
            assertEquals(100, storeConfiguration.maxEntries());
         }
      });
   }

}
