package org.infinispan.persistence.leveldb;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.leveldb.LevelDBStoreTest")
public class LevelDBStoreTest extends BaseStoreTest {

   private String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   protected LevelDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      LevelDBStoreConfigurationBuilder cfg = lcb.addStore(LevelDBStoreConfigurationBuilder.class);
      cfg.location(tmpDirectory + "/data");
      cfg.expiredLocation(tmpDirectory + "/expiry");
      cfg.clearThreshold(2);
      return cfg;
   }


   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      clearTempDir();
      LevelDBStore fcs = new LevelDBStore();
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence());
      fcs.init(createContext(cb.build()));
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
                  MarshalledEntry me = TestingUtil.marshalledEntry(entry, getMarshaller());
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
}
