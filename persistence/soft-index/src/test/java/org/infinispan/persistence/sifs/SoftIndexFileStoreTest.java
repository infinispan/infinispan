package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Low level single-file cache store tests.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "unit", testName = "persistence.SoftIndexFileStoreTest")
public class SoftIndexFileStoreTest extends BaseStoreTest {

   SoftIndexFileStore store;
   String tmpDirectory;
   boolean startIndex = true;
   boolean keepIndex = false;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      store = new SoftIndexFileStore() {
         boolean firstTime = true;

         @Override
         protected void startIndex() {
            if (startIndex) {
               super.startIndex();
            }
         }

         @Override
         public void start() {
            super.start();
            if (!firstTime) {
               assertEquals(keepIndex, isIndexLoaded());
            }
            firstTime = false;
         }

         @Override
         public void stop() {
            super.stop();
            if (!keepIndex) {
               for (File f : new File(tmpDirectory).listFiles()) {
                  if (!f.isDirectory()) {
                     f.delete();
                  }
               }
            }
         }
      };
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data")
            .maxFileSize(1000);

      store.init(createContext(builder.build()));
      return store;
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   public void testLoadUnload() {
      int numEntries = 10000;
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }
      for (int i = 0; i < numEntries; ++i) {
         assertNotNull(key(i), store.load(key(i)));
         assertTrue(key(i), store.delete(key(i)));
      }
      store.clear();
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(MarshalledEntryUtil.create(ice, getMarshaller()));
      }
      for (int i = numEntries - 1; i >= 0; --i) {
         assertNotNull(key(i), store.load(key(i)));
         assertTrue(key(i), store.delete(key(i)));
      }
   }

   // test for ISPN-5658
   public void testStopStartAndMultipleWrites() {
      MarshalledEntry<Object, Object> entry1 = marshalledEntry(internalCacheEntry("k1", "v1", -1));
      MarshalledEntry<Object, Object> entry2 = marshalledEntry(internalCacheEntry("k1", "v2", -1));

      store.write(entry1);
      store.write(entry1);
      store.write(entry1);

      store.stop();
      store.start();

      MarshalledEntry entry = store.load("k1");
      assertNotNull(entry);
      assertEquals("v1", entry.getValue());
      store.write(entry2);

      store.stop();
      store.start();

      entry = store.load("k1");
      assertNotNull(entry);
      assertEquals("v2", entry.getValue());
   }

   // test for ISPN-5743
   public void testStopStartWithRemoves() {
      String KEY = "k1";
      MarshalledEntry<Object, Object> entry1 = marshalledEntry(internalCacheEntry(KEY, "v1", -1));
      MarshalledEntry<Object, Object> entry2 = marshalledEntry(internalCacheEntry(KEY, "v2", -1));

      store.write(entry1);
      store.delete(KEY);

      store.stop();
      store.start();

      assertNull(store.load(KEY));
      store.write(entry2);
      store.delete(KEY);
      store.write(entry1);

      store.stop();
      startIndex = false;
      store.start();

      assertEquals(entry1.getValue(), store.load(KEY).getValue());
      startIndex = true;
      store.startIndex();
   }

   // test for ISPN-5753
   public void testOverrideWithExpirableAndCompaction() throws InterruptedException {
      // write immortal entry
      store.write(marshalledEntry(internalCacheEntry("key", "value1", -1)));
      writeGibberish(); // make sure that compaction happens - value1 is evacuated
      log.debug("Size :" + store.size());
      store.write(marshalledEntry(internalCacheEntry("key", "value2", 1)));
      timeService.advance(2);
      writeGibberish(); // make sure that compaction happens - value2 expires
      store.stop();
      store.start();
      // value1 has been overwritten and value2 has expired
      MarshalledEntry entry = store.load("key");
      assertNull(entry != null ? entry.getKey() + "=" + entry.getValue() : null, entry);
   }

   public void testStopStartWithLoadDoesNotNukeValues() throws InterruptedException, PersistenceException {
      keepIndex = true;
      try {
         testStopStartDoesNotNukeValues();
      } finally {
         keepIndex = false;
      }
   }

   private void writeGibberish() {
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("foo", "bar", -1)));
         store.delete("foo");
      }
   }

   private String key(int i) {
      return String.format("key%010d", i);
   }

}
