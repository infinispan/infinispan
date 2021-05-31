package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.CompletionStages;
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

   SingleFileStore<Object, Object> store;
   String tmpDirectory;
   private TestObjectStreamMarshaller marshaller;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @BeforeMethod
   public void setUp() throws Exception {
      clearTempDir();
      store = new SingleFileStore<>();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder
            .persistence()
               .addStore(SingleFileStoreConfigurationBuilder.class)
                  .location(this.tmpDirectory)
                  .segmented(false)
                  .maxEntries(1);

      marshaller = new TestObjectStreamMarshaller();
      CompletionStages.join(store.start(PersistenceMockUtil.createContext(getClass(), builder.build(), marshaller)));
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

   public void testStoreSizeExceeded() {
      assertStoreSize(0, 0);
      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      try {
         store.write(0, MarshalledEntryUtil.create(1, "v1", sm));
         store.write(0, MarshalledEntryUtil.create(1, "v2", sm));
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
