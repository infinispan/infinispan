package org.infinispan.persistence.rocksdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreTest")
public class RocksDBStoreTest extends BaseNonBlockingStoreTest {

   private String tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   private boolean segmented;

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   public RocksDBStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RocksDBStoreTest().segmented(false),
            new RocksDBStoreTest().segmented(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder cb) {
      // Lower number of segments as it takes much longer to start up store otherwise (makes test take a long time otherwise)
      cb.clustering().hash().numSegments(16);
      createCacheStoreConfig(cb.persistence());
      return cb.build();
   }

   protected RocksDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      RocksDBStoreConfigurationBuilder cfg = lcb.addStore(RocksDBStoreConfigurationBuilder.class);
      cfg.segmented(segmented);
      cfg.location(tmpDirectory);
      cfg.expiredLocation(tmpDirectory);
      cfg.clearThreshold(2);
      return cfg;
   }


   @Override
   protected NonBlockingStore createStore() {
      clearTempDir();
      return new RocksDBStore();
   }

   @Test(groups = "stress")
   public void testConcurrentWrite() throws InterruptedException {
      final int THREADS = 8;
      final AtomicBoolean run = new AtomicBoolean(true);
      final AtomicInteger written = new AtomicInteger();
      final CountDownLatch started = new CountDownLatch(THREADS);
      final CountDownLatch finished = new CountDownLatch(THREADS);
      for (int i = 0; i < THREADS; ++i) {
         final int thread = i;
         fork(() -> {
            try {
               started.countDown();
               int i1 = 0;
               while (run.get()) {
                  InternalCacheEntry entry = TestInternalCacheEntryFactory.create("k" + i1, "v" + i1);
                  MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());
                  try {
                     store.write(me);
                     ++i1;
                     int prev;
                     do {
                        prev = written.get();
                        if ((prev & (1 << thread)) != 0) break;
                     } while (written.compareAndSet(prev, prev | (1 << thread)));
                  } catch (PersistenceException e) {
                     // when the store is stopped, exceptions are thrown
                  }
               }
            } catch (Exception e) {
               log.error("Failed", e);
               throw new RuntimeException(e);
            } finally {
               finished.countDown();
            }
         });
      }
      if (finished.await(1, TimeUnit.SECONDS)) {
         fail("Test shouldn't have finished yet");
      }
      run.set(false);
      if (!finished.await(30, TimeUnit.SECONDS)) {
         fail("Test should have finished!");
      }
      assertEquals(written.get(), (1 << THREADS) - 1, "pre");
   }

   /**
    * Test to make sure that when segments are added or removed that there are no issues
    */
   public void testSegmentsRemovedAndAdded() {
      Object key = "first-key";
      Object value = "some-value";
      int segment = keyPartitioner.getSegment(key);

      InternalCacheEntry entry = TestInternalCacheEntryFactory.create(key, value);
      MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());

      store.write(me);

      assertTrue(store.contains(key));

      // Now remove the segment that held our key
      CompletionStages.join(store.removeSegments(IntSets.immutableSet(segment)));

      assertFalse(store.contains(key));

      // Now add the segment back
      CompletionStages.join(store.addSegments(IntSets.immutableSet(segment)));

      store.write(me);

      assertTrue(store.contains(key));
   }

   // deleteRange uses a magic begin and end key. we assert that everything was removed
   public void testDeleteRange() {
      for (int i = 0; i < 100; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         InternalCacheEntry entry = TestInternalCacheEntryFactory.create(key, value);
         MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());
         store.write(me);
         assertTrue(store.contains(key));
      }

      store.clear();

      for (int i = 0; i < 100; i++) {
         String key = "key-" + i;
         boolean contains = store.contains(key);
         assertFalse(contains);
      }
   }

   // deleteRange uses a magic begin and end key. we assert that only the data from the give segment was removed
   public void testDeleteRangeForGivenSegment() {
      int segmentUnderTest = 1;
      List<String> keysInTheSegment = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         InternalCacheEntry entry = TestInternalCacheEntryFactory.create(key, value);
         MarshallableEntry me = MarshalledEntryUtil.create(entry, getMarshaller());
         int segmentFromKey = store.getKeyPartitioner().getSegment(entry.getKey());
         if (segmentFromKey == segmentUnderTest) {
            keysInTheSegment.add(key);
         }
         store.write(me);
      }

      store.removeSegments(IntSets.immutableSet(segmentUnderTest));

      for (int i = 0; i < 100; i++) {
         String key = "key-" + i;
         boolean contains = store.contains(key);
         if (keysInTheSegment.contains(key)) {
            assertFalse(contains);
         } else {
            assertTrue(contains);
         }
      }
   }
}
