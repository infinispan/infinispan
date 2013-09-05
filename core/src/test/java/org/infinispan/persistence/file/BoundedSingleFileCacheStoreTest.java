package org.infinispan.persistence.file;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.DummyLoaderContext;
import org.infinispan.persistence.DummyLoaderContext;
import org.infinispan.persistence.MarshalledEntryImpl;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests for single-file cache store when configured to be bounded.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "loaders.file.BoundedSingleFileCacheStoreTest")
public class BoundedSingleFileCacheStoreTest extends AbstractInfinispanTest {

   SingleFileStore store;
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
      store = new SingleFileStore();
      SingleFileStoreConfiguration fileStoreConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory)
                  .maxEntries(1)
                  .create();
      store.init(new DummyLoaderContext(fileStoreConfiguration, getCache(), getMarshaller()));
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
      store.write(new MarshalledEntryImpl(1, "v1", null, new TestObjectStreamMarshaller()));
      store.write(new MarshalledEntryImpl(2, "v2", null, new TestObjectStreamMarshaller()));
      assertStoreSize(1, 1);
   }

   private void assertStoreSize(int expectedEntries, int expectedFree) {
      assertEquals("Entries: " + store.getEntries(), expectedEntries, store.getEntries().size());
      assertEquals("Free: " + store.getFreeList(), expectedFree, store.getFreeList().size());
   }

   Cache getCache() {
      return BaseCacheStoreTest.mockCache("mockCache-" + getClass().getName());
   }

   StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }

}
