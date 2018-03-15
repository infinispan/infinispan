package org.infinispan.persistence.file;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Single file cache store functional test.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = {"unit", "smoke"}, testName = "persistence.file.SingleFileStoreFunctionalTest")
public class SingleFileStoreFunctionalTest extends BaseStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      Util.recursiveFileRemove(tmpDirectory);
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
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
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <file-store shared=\"false\" preload=\"true\"/> \n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>");
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
      Util.recursiveFileRemove("Infinispan-SingleFileStore");
   }

   public void testParsingElement() throws Exception {
      String config = TestingUtil.wrapXMLWithoutSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <persistence passivation=\"false\"> \n" +
            "         <file-store path=\"other-location\" max-entries=\"100\" shared=\"false\" preload=\"true\" fragmentation-factor=\"0.75\"/> \n" +
            "      </persistence>\n" +
            "   </local-cache>\n" +
            "</cache-container>");
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
            assertEquals(0.75f, store.getConfiguration().fragmentationFactor(), 0f);
         }
      });
      Util.recursiveFileRemove("other-location");
   }

}
