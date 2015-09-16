package org.infinispan.persistence.sifs;

import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.junit.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.AssertJUnit;
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

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass
   protected void clearTempDir() {
      recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      store = new SoftIndexFileStore() {
         @Override
         protected void startIndex() {
            if (startIndex) {
               super.startIndex();
            }
         }
      };
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      builder.persistence()
               .addStore(SoftIndexFileStoreConfigurationBuilder.class)
                  .indexLocation(tmpDirectory).dataLocation(tmpDirectory + "/data");

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
         store.write(new MarshalledEntryImpl(ice.getKey(), ice.getValue(), internalMetadata(ice), getMarshaller()));
      }
      System.out.println("Loaded all entries");
      for (int i = 0; i < numEntries; ++i) {
         if (!store.delete(key(i))) {
            AssertJUnit.fail("Key " + key(i) + " not found");
         }
      }
      store.clear();
      for (int i = 0; i < numEntries; ++i) {
         InternalCacheEntry ice = TestInternalCacheEntryFactory.create(key(i), "value" + i);
         store.write(new MarshalledEntryImpl(ice.getKey(), ice.getValue(), internalMetadata(ice), getMarshaller()));
      }
      for (int i = numEntries - 1; i >= 0; --i) {
         if (!store.delete(key(i))) {
            AssertJUnit.fail("Key " + key(i) + " not found");
         }
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

      assertEquals("v1", store.load("k1").getValue());
      store.write(entry2);

      store.stop();
      store.start();

      assertEquals("v2", store.load("k1").getValue());
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

   private String key(int i) {
      return String.format("key%010d", i);
   }

}
