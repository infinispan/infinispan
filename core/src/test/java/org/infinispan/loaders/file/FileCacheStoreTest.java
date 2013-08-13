package org.infinispan.loaders.file;

import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "loaders.file.FileCacheStoreTest")
public class FileCacheStoreTest extends BaseCacheStoreTest {

   FileCacheStore fcs;
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
   protected CacheStore createCacheStore() throws CacheLoaderException {
      clearTempDir();
      fcs = new FileCacheStore();

      FileCacheStoreConfigurationBuilder fileBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false)
            .loaders().addFileCacheStore();
      fileBuilder.fetchPersistentState(true)
            .location(this.tmpDirectory)
            .purgeSynchronously(true);
      fcs.init(fileBuilder.create(), getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }

   public void testStoreSizeExceeded() throws Exception {
      FileCacheStore store = createBoundedFileCacheStore();
      try {
         assertStoreSize(store, 0, 0);
         store.store(TestInternalCacheEntryFactory.create(1, "v1"));
         store.store(TestInternalCacheEntryFactory.create(2, "v2"));
         assertStoreSize(store, 1, 1);
      } finally {
         stopFileCacheStore(store);
      }
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

   private void assertStoreSize(FileCacheStore store, int expectedEntries, int expectedFree) {
      assertEquals("Entries: " + store.getEntries(), expectedEntries, store.getEntries().size());
      assertEquals("Free: " + store.getFreeList(), expectedFree, store.getFreeList().size());
   }

   private FileCacheStore createBoundedFileCacheStore() throws Exception {
      FileCacheStore store = new FileCacheStore();
      FileCacheStoreConfiguration fileStoreConfiguration = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
            .addFileCacheStore()
            .location(this.tmpDirectory)
            .maxEntries(1)
            .purgeSynchronously(true)
            .create();
      store.init(fileStoreConfiguration, getCache(), getMarshaller());
      store.start();
      return store;
   }

   private void stopFileCacheStore(FileCacheStore store) throws CacheLoaderException {
      if (store != null) {
         store.clear();
         store.stop();
      }
   }

}
