package org.infinispan.persistence.rocksdb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreTest")
public class RocksDBStoreTest extends BaseStoreTest {

   private String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   private Configuration configuration;
   private KeyPartitioner keyPartitioner;
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

   protected RocksDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      RocksDBStoreConfigurationBuilder cfg = lcb.addStore(RocksDBStoreConfigurationBuilder.class);
      cfg.segmented(segmented);
      cfg.location(tmpDirectory + "/data");
      cfg.expiredLocation(tmpDirectory + "/expiry");
      cfg.clearThreshold(2);
      return cfg;
   }


   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      RocksDBStore fcs = new RocksDBStore();
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      // Lower number of segments as it takes much longer to start up store otherwise (makes test take a long time otherwise)
      cb.clustering().hash().numSegments(16);
      createCacheStoreConfig(cb.persistence());

      configuration = cb.build();
      InitializationContext ctx = createContext(configuration);
      Cache cache = ctx.getCache();
      HashFunctionPartitioner partitioner = new HashFunctionPartitioner();
      partitioner.init(cache.getCacheConfiguration().clustering().hash());
      keyPartitioner = partitioner;
      cache.getAdvancedCache().getComponentRegistry().registerComponent(partitioner, KeyPartitioner.class);
      fcs.init(ctx);
      return fcs;
   }

   @Test(groups = "stress", timeOut = 15*60*1000)
   public void testConcurrentWriteAndRestart() {
      concurrentWriteAndRestart(true);
   }

   @Test(groups = "stress", timeOut = 15*60*1000)
   public void testConcurrentWriteAndStop() {
      concurrentWriteAndRestart(true);
   }

   private void concurrentWriteAndRestart(boolean start) {
      final int THREADS = 4;
      final AtomicBoolean run = new AtomicBoolean(true);
      final AtomicInteger writtenPre = new AtomicInteger();
      final AtomicInteger writtenPost = new AtomicInteger();
      final AtomicBoolean post = new AtomicBoolean(false);
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
                     AtomicInteger record = post.get() ? writtenPost : writtenPre;
                     cl.write(me);
                     ++i1;
                     int prev;
                     do {
                        prev = record.get();
                        if ((prev & (1 << thread)) != 0) break;
                     } while (record.compareAndSet(prev, prev | (1 << thread)));
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
      try {
         if (!started.await(30, TimeUnit.SECONDS)) {
            fail();
         }
         Thread.sleep(1000);
         cl.stop();
         post.set(true);
         Thread.sleep(1000);
         if (start) {
            cl.start();
            Thread.sleep(1000);
         }
      } catch (InterruptedException e) {
         fail();
      } finally {
         run.set(false);
      }
      try {
         if (!finished.await(30, TimeUnit.SECONDS)) {
            fail();
         }
      } catch (InterruptedException e) {
         fail();
      }
      assertEquals(writtenPre.get(), (1 << THREADS) - 1, "pre");
      if (start) {
         assertEquals(writtenPost.get(), (1 << THREADS) - 1, "post");
      } else {
         assertEquals(writtenPost.get(), 0, "post");
      }
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

      cl.write(me);

      assertTrue(cl.contains(key));

      SegmentedAdvancedLoadWriteStore salws = (SegmentedAdvancedLoadWriteStore) cl;
      // Now remove the segment that held our key
      salws.removeSegments(IntSets.immutableSet(segment));

      assertFalse(cl.contains(key));

      // Now add the segment back
      salws.addSegments(IntSets.immutableSet(segment));

      cl.write(me);

      assertTrue(cl.contains(key));
   }
}
