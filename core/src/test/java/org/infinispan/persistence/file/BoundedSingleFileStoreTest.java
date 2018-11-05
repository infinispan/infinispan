package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryImpl;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
   private TestObjectStreamMarshaller marshaller;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @BeforeMethod
   public void setUp() throws Exception {
      clearTempDir();
      store = new SingleFileStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder
            .persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory)
                  .maxEntries(1);

      marshaller = new TestObjectStreamMarshaller();
      store.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller));
      store.start();
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      try {
         if (store != null) {
            store.clear();
            store.stop();
         }
         marshaller.stop();
      } finally {
         store = null;
      }
   }

   public void testStoreSizeExceeded() throws Exception {
      assertStoreSize(0, 0);
      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      try {
         store.write(new MarshalledEntryImpl(1, "v1", null, sm));
         store.write(new MarshalledEntryImpl(2, "v2", null, sm));
         assertStoreSize(1, 1);
      } finally {
         sm.stop();
      }
   }

   private void assertStoreSize(int expectedEntries, int expectedFree) {
      assertEquals("Entries: " + store.getEntries(), expectedEntries, store.getEntries().size());
      assertEquals("Free: " + store.getFreeList(), expectedFree, store.getFreeList().size());
   }

}
