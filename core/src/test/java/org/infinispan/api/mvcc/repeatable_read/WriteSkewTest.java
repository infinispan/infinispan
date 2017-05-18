package org.infinispan.api.mvcc.repeatable_read;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.atomic.FineGrainedAtomicMap;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.WriteSkewTest")
public class WriteSkewTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(WriteSkewTest.class);
   protected TransactionManager tm;
   protected LockManager lockManager;
   protected EmbeddedCacheManager cacheManager;
   protected volatile Cache<String, String> cache;

   @BeforeClass
   public void setUp() {
      ConfigurationBuilder configurationBuilder = createConfigurationBuilder();
      configurationBuilder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      // The default cache is NOT write skew enabled.
      cacheManager = TestCacheManagerFactory.createCacheManager(configurationBuilder);
      configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      configurationBuilder.clustering().hash().groups().enabled();
      cacheManager.defineConfiguration("writeSkew", configurationBuilder.build());
   }

   protected ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .locking()
            .lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .isolationLevel(IsolationLevel.REPEATABLE_READ);
      return configurationBuilder;
   }

   @AfterClass
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
      cache =null;
      lockManager = null;
      tm = null;
   }

   @BeforeMethod
   public void postStart() {
      cache = cacheManager.getCache("writeSkew");
      lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      tm = TestingUtil.extractComponentRegistry(cache).getComponent(TransactionManager.class);
   }

   protected void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException {
      tm.commit();
   }

   protected void assertNoLocks() {
      LockAssert.assertNoLocks(lockManager);
   }

   public void testDontCheckWriteSkew() throws Exception {
      // Use the default cache here.
      cache = cacheManager.getCache();
      lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      tm = TestingUtil.extractComponentRegistry(cache).getComponent(TransactionManager.class);

      doTest(true);
   }

   public void testCheckWriteSkew() throws Exception {
      doTest(false);
   }

   /**
    * Tests write skew with two concurrent transactions that each execute two put() operations. One put() is done on the
    * same key to create a write skew. The second put() is only needed to avoid optimizations done by
    * OptimisticLockingInterceptor for single modification transactions and force it reach the code path that triggers
    * ISPN-2092.
    */
   public void testCheckWriteSkewWithMultipleModifications() throws Exception {
      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      final CountDownLatch latch3 = new CountDownLatch(1);

      Future<Void> t1 = fork(() -> {
         latch1.await();

         tm.begin();
         try {
            try {
               cache.get("k1");
               cache.put("k1", "v1");
               cache.put("k2", "thread 1");
            } finally {
               latch2.countDown();
            }
            latch3.await();
            Exceptions.expectException(RollbackException.class, this::commit);
         } catch (Exception e) {
            log.error("Unexpected exception in transaction 1", e);
            tm.rollback();
         }
         return null;
      });

      Future<Void> t2 = fork(() -> {
         latch2.await();

         tm.begin();
         try {
            try {
               cache.get("k1");
               cache.put("k1", "v2");
               cache.put("k3", "thread 2");
               commit();
            } finally {
               latch3.countDown();
            }
         } catch (Exception e) {
            // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
            if (tm.getTransaction() != null) {
               try {
                  tm.rollback();
               } catch (SystemException e1) {
                  log.error("Failed to rollback", e1);
               }
            }

            // Pass the exception to the main thread
            throw e;
         }
         return null;
      });

      latch1.countDown();

      t1.get(10, SECONDS);
      t2.get(10, SECONDS);

      assertTrue("k1 is expected to be in cache.", cache.containsKey("k1"));
      assertEquals("Wrong value for key k1.", "v2", cache.get("k1"));
   }

   /** Checks that multiple modifications compare the initial value and the write skew does not fire */
   public void testNoWriteSkewWithMultipleModifications() throws Exception {
      cache.put("k1", "init");
      tm.begin();
      assertEquals("init", cache.get("k1"));
      cache.put("k1", "v2");
      cache.put("k2", "v3");
      commit();
   }

   /**
    * Verifies we can insert and then remove a value in the same transaction.
    * See also ISPN-2075.
    */
   public void testDontFailOnImmediateRemoval() throws Exception {
      final String key = "testDontOnImmediateRemoval-Key";

      tm.begin();
      cache.put(key, "testDontOnImmediateRemoval-Value");
      assertEquals("Wrong value for key " + key, "testDontOnImmediateRemoval-Value", cache.get(key));
      cache.put(key, "testDontOnImmediateRemoval-Value-Second");
      cache.remove(key);
      commit();
      assertFalse("Key " + key + " was not removed as expected.", cache.containsKey(key));
   }

   /**
    * Verifies we can create a new AtomicMap, use it and then remove it while in the same transaction
    * See also ISPN-2075.
    */
   public void testDontFailOnImmediateRemovalOfAtomicMaps() throws Exception {
      final String key = "key1";
      final String subKey = "subK";
      TestingUtil.withTx(tm, () -> {
         FineGrainedAtomicMap<String, String> fineGrainedAtomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache, key);
         fineGrainedAtomicMap.put(subKey, "some value");
         fineGrainedAtomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache, key);
         fineGrainedAtomicMap.get(subKey);
         fineGrainedAtomicMap.put(subKey, "v");
         fineGrainedAtomicMap.put(subKey + 2, "v2");
         fineGrainedAtomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache, key);
         Object object = fineGrainedAtomicMap.get(subKey);
         assertEquals("Wrong FGAM sub-key value.", "v", object);
         AtomicMapLookup.removeAtomicMap(cache, key);
         return null;
      });
   }

   public void testNoWriteSkew() throws Exception {
      //simplified version of testWriteSkewWithOnlyPut
      final String key = "k";
      tm.begin();
      try {
         cache.put(key, "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            commit();
         } else {
            tm.rollback();
         }
      }
      Cache<String, String> putCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);

      tm.begin();
      putCache.put(key, "v1");
      final Transaction tx1 = tm.suspend();

      tm.begin();
      putCache.put(key, "v2");
      final Transaction tx2 = tm.suspend();

      tm.begin();
      putCache.put(key, "v3");
      final Transaction tx3 = tm.suspend();

      //the following commits should not fail the write skew check
      tm.resume(tx1);
      commit();

      tm.resume(tx2);
      commit();

      tm.resume(tx3);
      commit();
   }

   public void testWriteSkew() throws Exception {
      //simplified version of testWriteSkewWithOnlyPut
      final String key = "k";
      tm.begin();
      try {
         cache.put(key, "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            commit();
         } else {
            tm.rollback();
         }
      }

      tm.begin();
      cache.put(key, "v1");
      final Transaction tx1 = tm.suspend();

      tm.begin();
      cache.put(key, "v2");
      final Transaction tx2 = tm.suspend();

      tm.begin();
      cache.put(key, "v3");
      final Transaction tx3 = tm.suspend();

      //the first commit should succeed
      tm.resume(tx1);
      commit();

      //the remaining should fail
      try {
         tm.resume(tx2);
         commit();
         fail("Transaction should fail!");
      } catch (RollbackException e) {
         //expected
      }

      try {
         tm.resume(tx3);
         commit();
         fail("Transaction should fail!");
      } catch (RollbackException e) {
         //expected
      }
   }

   // Write skew should not fire when the read is based purely on previously written value
   // (the first put does not read the value)
   // This test actually tests only local write skew check
   public void testPreviousValueIgnored() throws Exception {
      cache.put("k", "init");

      tm.begin();
      cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put("k", "v1");
      assertEquals("v1", cache.put("k", "v2"));
      Transaction tx = tm.suspend();

      assertEquals("init", cache.put("k", "other"));

      tm.resume(tx);
      commit();
   }

   public void testWriteSkewWithOnlyPut() throws Exception {
      tm.begin();
      try {
         cache.put("k", "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) commit();
         else tm.rollback();
      }

      int nbWriters = 10;
      CyclicBarrier barrier = new CyclicBarrier(nbWriters + 1);
      List<Future<Void>> futures = new ArrayList<>(nbWriters);
      for (int i = 0; i < nbWriters; i++) {
         log.debug("Schedule execution");
         Future<Void> future = fork(new EntryWriter(barrier));
         futures.add(future);
      }
      barrier.await(); // wait for all threads to be ready
      barrier.await(); // wait for all threads to finish

      log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
      for (Future<Void> future : futures) future.get();
   }


   private void doTest(final boolean disabledWriteSkewCheck) throws Exception {
      final String key = "k";
      final CountDownLatch w1Signal = new CountDownLatch(1);
      final CountDownLatch w2Signal = new CountDownLatch(1);
      final CountDownLatch threadSignal = new CountDownLatch(2);

      cache.put(key, "v");

      Future<Void> w1 = fork(() -> {
         tm.begin();
         assertEquals("Wrong value in Writer-1 for key " + key + ".", "v", cache.get(key));
         threadSignal.countDown();
         w1Signal.await();
         cache.put(key, "v2");
         commit();
         return null;
      });

      Future<Void> w2 = fork(() -> {
         tm.begin();
         assertEquals("Wrong value in Writer-2 for key " + key + ".", "v", cache.get(key));
         threadSignal.countDown();
         w2Signal.await();
         cache.put(key, "v3");
         if (disabledWriteSkewCheck) {
            commit();
         } else {
            Exceptions.expectException(RollbackException.class, this::commit);
         }
         return null;
      });

      threadSignal.await(10, SECONDS);
      // now.  both txs have read.
      // let tx1 start writing
      w1Signal.countDown();
      w1.get(10, SECONDS);

      w2Signal.countDown();
      w2.get(10, SECONDS);

      if (disabledWriteSkewCheck) {
         assertEquals("W2 should have overwritten W1's work!", "v3", cache.get(key));
         assertNoLocks();
      } else {
         assertEquals("W2 should *not* have overwritten W1's work!", "v2", cache.get(key));
         assertNoLocks();
      }
   }

   protected class EntryWriter implements Callable<Void> {
      private final CyclicBarrier barrier;

      EntryWriter(CyclicBarrier barrier) {
         this.barrier = barrier;
      }

      @Override
      public Void call() throws Exception {
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();

            tm.begin();
            try {
               cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put("k", "_lockthisplease_");
            } catch (Exception e) {
               log.error("Unexpected", e);
               tm.setRollbackOnly();
               throw e;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) commit();
               else tm.rollback();
            }

            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }
}
