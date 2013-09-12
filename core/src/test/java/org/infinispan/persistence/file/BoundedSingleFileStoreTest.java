package org.infinispan.persistence.file;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.SingleFileStoreConfiguration;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.marshall.core.MarshalledEntryImpl;
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
@Test(groups = "unit", testName = "persistence.file.BoundedSingleFileStoreTest")
public class BoundedSingleFileStoreTest extends AbstractInfinispanTest {

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
      store.init(new DummyInitializationContext(fileStoreConfiguration, getCache(), getMarshaller(),
                                                new ByteBufferFactoryImpl(),
                                                new MarshalledEntryFactoryImpl(getMarshaller())));
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
      return BaseStoreTest.mockCache("mockCache-" + getClass().getName());
   }

   StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }

}
