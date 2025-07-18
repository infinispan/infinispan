package org.infinispan.persistence.rocksdb;

import static java.util.Collections.singletonList;
import static org.infinispan.commons.util.IntSets.immutableSet;
import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.IntSet;
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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreTest")
public class RocksDBStoreTest extends BaseNonBlockingStoreTest {

   private final String tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   private boolean segmented;
   public static final String KEY_1 = "key1";
   public static final String KEY_2 = "key2";

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
      createCacheStoreConfig(cb.persistence());
      return cb.build();
   }

   protected RocksDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      RocksDBStoreConfigurationBuilder cfg = lcb.addStore(RocksDBStoreConfigurationBuilder.class);
      cfg.segmented(segmented);
      cfg.location(tmpDirectory);
      cfg.expiredLocation(tmpDirectory);
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
      assertEquals("pre", (1 << THREADS) - 1, written.get());
   }

   /**
    * Test to make sure that when segments are added or removed that there are no issues
    */
   public void testSegmentsRemovedAndAdded() {
      int segment1 = keyPartitioner.getSegment(KEY_1);
      MarshallableEntry me1 = marshallableEntryFactory.create(KEY_1, "value1");
      store.write(me1);
      assertTrue(join(store.containsKey(segment1, KEY_1)));

      int segment2 = keyPartitioner.getSegment(KEY_2);
      AssertJUnit.assertTrue(segment1 != segment2);
      MarshallableEntry me2 = marshallableEntryFactory.create(KEY_2, "value2");
      store.write(me2);
      assertTrue(join(store.containsKey(segment2, KEY_2)));
      assertEquals(Arrays.asList(KEY_1, KEY_2), listKeys(null));

      store.removeSegments(immutableSet(segment1));

      assertEquals(0, (long) join(store.size(immutableSet(segment1))));

      assertFalse(join(store.containsKey(segment1, KEY_1)));
      assertEmpty(immutableSet(segment1));

      assertTrue(join(store.containsKey(segment2, KEY_2)));
      assertEquals(1, (long) join(store.size(immutableSet(segment2))));

      assertEquals(singletonList(KEY_2), listKeys(null));

      // Now add the segment back
      join(store.addSegments(immutableSet(segment1)));

      store.write(me1);

      assertTrue(store.contains(KEY_1));
      assertEquals(Arrays.asList(KEY_1, KEY_2), listKeys(null));
   }

   public void testClear() {
      MarshallableEntry me1 = marshallableEntryFactory.create(KEY_1, "value");
      store.write(1, me1);
      assertTrue(join(store.containsKey(1, KEY_1)));

      // clear() uses RockDB's DeleteRange call internally
      // Create a fake key that is after the end key of this DeleteRange call
      // A custom marshaller could in theory create the same kind of key, but this is simpler
      // because we don't need to unmarshal the key
      int keySize = 10000;
      byte[] keyBytes = new byte[keySize];
      Arrays.fill(keyBytes, (byte) 0xff);
      byte[] valueBytes = Util.EMPTY_BYTE_ARRAY;
      MarshallableEntry me2 = marshallableEntryFactory.create(ByteBufferImpl.create(keyBytes), ByteBufferImpl.create(valueBytes));
      store.write(1, me2);

      // Because key2 cannot be unmarshalled, we cannot confirm the write with contains(key2) or even with size()

      store.clear();

      assertFalse(join(store.containsKey(1, KEY_1)));

      assertEmpty(null);
   }

   private void assertEmpty(IntSet segments) {
      assertEquals(0, (long) join(store.size(segments)));

      assertEquals(Collections.emptyList(), listKeys(segments));
   }

   private List<Object> listKeys(IntSet segments) {
      return Flowable.fromPublisher(store.publishEntries(segments, null, true))
                     .map(MarshallableEntry::getKey)
                     .toSortedList()
                     .blockingGet();
   }
}
