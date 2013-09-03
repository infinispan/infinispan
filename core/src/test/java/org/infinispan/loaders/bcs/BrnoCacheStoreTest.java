package org.infinispan.loaders.bcs;

import org.infinispan.configuration.cache.BrnoCacheStoreConfiguration;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.testng.AssertJUnit.fail;

/**
 * Low level single-file cache store tests.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "loaders.file.BrnoCacheStoreTest")
public class BrnoCacheStoreTest extends BaseCacheStoreTest {

   BrnoCacheStore store;
   String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected CacheStore createCacheStore() throws Exception {
      clearTempDir();
      store = new BrnoCacheStore();
      BrnoCacheStoreConfiguration configuration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addBrnoCacheStore()
                  .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
                  .purgeSynchronously(true)
                  .create();
      store.init(configuration, getCache(), getMarshaller());
      store.start();
      return store;
   }

   @Override
   @Test(enabled = false)
   public void testStreamingAPI() throws IOException, CacheLoaderException {
      // streaming API not really used by production code (except by decorators)
   }

   @Override
   @Test(enabled = false)
   public void testStreamingAPIReusingStreams() throws IOException, CacheLoaderException {
      // streaming API not really used by production code (except by decorators)
   }

   public void testLoadUnload() throws CacheLoaderException {
      int numEntries = 10000;
      for (int i = 0; i < numEntries; ++i) {
         store.store(TestInternalCacheEntryFactory.create(key(i), "value" + i));
      }
      System.out.println("Loaded all entries");
      for (int i = 0; i < numEntries; ++i) {
         if (!store.remove(key(i))) {
            fail("Key " + key(i) + " not found");
         }
      }
      store.clear();
      for (int i = 0; i < numEntries; ++i) {
         store.store(TestInternalCacheEntryFactory.create(key(i), "value" + i));
      }
      for (int i = numEntries - 1; i >= 0; --i) {
         if (!store.remove(key(i))) {
            fail("Key " + key(i) + " not found");
         }
      }
   }

   private String key(int i) {
      return String.format("key%010d", i);
   }

}
