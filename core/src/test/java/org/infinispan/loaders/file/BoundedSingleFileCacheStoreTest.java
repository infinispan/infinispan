package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.SingleFileCacheStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileCacheStoreConfigurationBuilder;
import org.infinispan.loaders.AbstractCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.testng.AssertJUnit.*;

/**
 * Tests for single-file cache store when configured to be bounded.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "loaders.file.BoundedSingleFileCacheStoreTest")
public class BoundedSingleFileCacheStoreTest extends AbstractInfinispanTest {

   SingleFileCacheStore store;
   String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @BeforeMethod
   public void setUp() throws Exception {
      clearTempDir();
      store = new SingleFileCacheStore();
      SingleFileCacheStoreConfiguration fileStoreConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(SingleFileCacheStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory)
                  .maxEntries(1)
                  .purgeSynchronously(true)
                  .create();
      store.init(fileStoreConfiguration, getCache(), getMarshaller());
      store.start();
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      try {
         if (store != null) {
            store.clear();
            store.stop();
         }
      } finally {
         store = null;
      }
   }

   public void testStoreSizeExceeded() throws Exception {
      assertStoreSize(0, 0);
      store.store(TestInternalCacheEntryFactory.create(1, "v1"));
      store.store(TestInternalCacheEntryFactory.create(2, "v2"));
      assertStoreSize(1, 1);
   }

   private void assertStoreSize(int expectedEntries, int expectedFree) {
      assertEquals("Entries: " + store.getEntries(), expectedEntries, store.getEntries().size());
      assertEquals("Free: " + store.getFreeList(), expectedFree, store.getFreeList().size());
   }

   Cache getCache() {
      return AbstractCacheStoreTest.mockCache("mockCache-" + getClass().getName());
   }

   StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }

}
