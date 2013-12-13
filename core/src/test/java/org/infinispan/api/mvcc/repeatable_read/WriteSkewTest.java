package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.atomic.FineGrainedAtomicMap;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.*;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.WriteSkewTest")
public class WriteSkewTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(WriteSkewTest.class);
   protected TransactionManager tm;
   protected LockManager lockManager;
   protected EmbeddedCacheManager cacheManager;
   protected volatile Cache<String, String> cache;

   @BeforeTest
   public void setUp() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
         .locking()
            .lockAcquisitionTimeout(3000)
            .isolationLevel(IsolationLevel.REPEATABLE_READ);
      // The default cache is NOT write skew enabled.
      cacheManager = TestCacheManagerFactory.createCacheManager(configurationBuilder);
      configurationBuilder.locking().writeSkewCheck(true).versioning().enable().scheme(VersioningScheme.SIMPLE);
      cacheManager.defineConfiguration("writeSkew", configurationBuilder.build());
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
      cache =null;
      lockManager = null;
      tm = null;
   }

   private void postStart() {
      lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      tm = TestingUtil.extractComponentRegistry(cache).getComponent(TransactionManager.class);
   }

   protected void assertNoLocks() {
      LockAssert.assertNoLocks(lockManager);
   }

   private void setCacheWithWriteSkewCheck() {
      cache = cacheManager.getCache("writeSkew");
   }

   private void setCacheWithoutWriteSkewCheck() {
      // Use the default cache here.
      cache = cacheManager.getCache();
   }

   public void testDontCheckWriteSkew() throws Exception {
      setCacheWithoutWriteSkewCheck();
      postStart();
      doTest(true);
   }

   public void testCheckWriteSkew() throws Exception {
      setCacheWithWriteSkewCheck();
      postStart();
      doTest(false);
   }

   /**
    * Tests write skew with two concurrent transactions that each execute two put() operations. One put() is done on the
    * same key to create a write skew. The second put() is only needed to avoid optimizations done by
    * OptimisticLockingInterceptor for single modification transactions and force it reach the code path that triggers
    * ISPN-2092.
    *
    * @throws Exception
    */
   public void testCheckWriteSkewWithMultipleModifications() throws Exception {
      setCacheWithWriteSkewCheck();
      postStart();

      final AtomicInteger successes = new AtomicInteger();
      final AtomicInteger rollbacks = new AtomicInteger();

      final CountDownLatch latch1 = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);
      final CountDownLatch latch3 = new CountDownLatch(1);

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            try {
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
                  tm.commit(); //this is expected to fail
                  successes.incrementAndGet();
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rollbacks.incrementAndGet();
                  }

                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  if (tm.getTransaction() != null) {
                     try {
                        tm.rollback();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "Thread-1, WriteSkewTest");

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            try {
               latch2.await();

               tm.begin();
               try {
                  try {
                     cache.get("k1");
                     cache.put("k1", "v2");
                     cache.put("k3", "thread 2");
                     tm.commit();
                     successes.incrementAndGet();
                  } finally {
                     latch3.countDown();
                  }
               } catch (Exception e) {
                  if (e instanceof RollbackException) {
                     rollbacks.incrementAndGet();
                  }

                  // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                  if (tm.getTransaction() != null) {
                     try {
                        tm.rollback();
                     } catch (SystemException e1) {
                        log.error("Failed to rollback", e1);
                     }
                  }
                  throw e;
               }
            } catch (Exception ex) {
               ex.printStackTrace();
            }
         }
      }, "Thread-2, WriteSkewTest");

      t1.start();
      t2.start();
      latch1.countDown();
      t1.join();
      t2.join();

      log.trace("successes= " + successes.get());
      log.trace("rollbacks= " + rollbacks.get());

      assertTrue("k1 is expected to be in cache.", cache.containsKey("k1"));
      assertEquals("Wrong value for key k1.", "v2", cache.get("k1"));
      assertEquals("Expects only one thread to succeed.", 1, successes.get());
      assertEquals("Expects only one thread to fail", 1, rollbacks.get());
   }

   /**
    * Verifies we can insert and then remove a value in the same transaction.
    * See also ISPN-2075.
    */
   public void testDontFailOnImmediateRemoval() throws Exception {
      setCacheWithWriteSkewCheck();
      postStart();
      final String key = "testDontOnImmediateRemoval-Key";

      tm.begin();
      cache.put(key, "testDontOnImmediateRemoval-Value");
      assertEquals("Wrong value for key " + key, "testDontOnImmediateRemoval-Value", cache.get(key));
      cache.put(key, "testDontOnImmediateRemoval-Value-Second");
      cache.remove(key);
      tm.commit();
      assertFalse("Key " + key + " was not removed as expected.", cache.containsKey(key));
   }

   /**
    * Verifies we can create a new AtomicMap, use it and then remove it while in the same transaction
    * See also ISPN-2075.
    */
   public void testDontFailOnImmediateRemovalOfAtomicMaps() throws Exception {
      setCacheWithWriteSkewCheck();
      postStart();
      final String key = "key1";
      final String subKey = "subK";
      TestingUtil.withTx(tm, new Callable<Object>() {
         public Object call() {
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
         }
      });
   }

   public void testNoWriteSkew() throws Exception {
      //simplified version of testWriteSkewWithOnlyPut
      setCacheWithWriteSkewCheck();
      postStart();

      final String key = "k";
      tm.begin();
      try {
         cache.put(key, "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            tm.commit();
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
      tm.commit();

      tm.resume(tx2);
      tm.commit();

      tm.resume(tx3);
      tm.commit();
   }

   public void testWriteSkew() throws Exception {
      //simplified version of testWriteSkewWithOnlyPut
      setCacheWithWriteSkewCheck();
      postStart();

      final String key = "k";
      tm.begin();
      try {
         cache.put(key, "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            tm.commit();
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
      tm.commit();

      //the remaining should fail
      try {
         tm.resume(tx2);
         tm.commit();
         fail("Transaction should fail!");
      } catch (RollbackException e) {
         //expected
      }

      try {
         tm.resume(tx3);
         tm.commit();
         fail("Transaction should fail!");
      } catch (RollbackException e) {
         //expected
      }
   }

   public void testWriteSkewWithOnlyPut() throws Exception {
      setCacheWithWriteSkewCheck();
      postStart();

      tm.begin();
      try {
         cache.put("k", "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
         else tm.rollback();
      }

      int nbWriters = 10;
      CyclicBarrier barrier = new CyclicBarrier(nbWriters + 1);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(nbWriters);
      ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactory() {
         volatile int i = 0;
         @Override
         public Thread newThread(Runnable r) {
            int ii = i++;
            return new Thread(r, "EntryWriter-" + ii + ", WriteSkewTest");
         }
      });

      try {
         for (int i = 0; i < nbWriters; i++) {
            log.debug("Schedule execution");
            Future<Void> future = executorService.submit(new EntryWriter(barrier));
            futures.add(future);
         }
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish

         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
      } finally {
         executorService.shutdownNow();
      }
   }


   private void doTest(final boolean allowWriteSkew) throws Exception {
      final String key = "k";
      final Set<Exception> w1exceptions = new HashSet<Exception>();
      final Set<Exception> w2exceptions = new HashSet<Exception>();
      final CountDownLatch w1Signal = new CountDownLatch(1);
      final CountDownLatch w2Signal = new CountDownLatch(1);
      final CountDownLatch threadSignal = new CountDownLatch(2);

      cache.put(key, "v");

      Thread w1 = new Thread("Writer-1, WriteSkewTest") {
         @Override
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assertEquals("Wrong value in Writer-1 for key " + key + ".", "v", cache.get(key));
               threadSignal.countDown();
               didCoundDown = true;
               w1Signal.await();
               cache.put(key, "v2");
               tm.commit();
            }
            catch (Exception e) {
               w1exceptions.add(e);
            }
            finally {
               if (!didCoundDown) threadSignal.countDown();
            }
         }
      };

      Thread w2 = new Thread("Writer-2, WriteSkewTest") {
         @Override
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assertEquals("Wrong value in Writer-2 for key " + key + ".", "v", cache.get(key));
               threadSignal.countDown();
               didCoundDown = true;
               w2Signal.await();
               cache.put(key, "v3");
               tm.commit();
            }
            catch (Exception e) {
               w2exceptions.add(e);
               // the exception will be thrown when doing a cache.put().  We should make sure we roll back the tx to release locks.
               if (!allowWriteSkew) {
                  try {
                     tm.rollback();
                  }
                  catch (SystemException e1) {
                     // do nothing.
                  }
               }
            }
            finally {
               if (!didCoundDown) threadSignal.countDown();
            }
         }
      };

      w1.start();
      w2.start();

      threadSignal.await();
      // now.  both txs have read.
      // let tx1 start writing
      w1Signal.countDown();
      w1.join();

      w2Signal.countDown();
      w2.join();

      if (allowWriteSkew) {
         // should have no exceptions!!
         throwExceptions(w1exceptions);
         throwExceptions(w2exceptions);
         assertEquals("W2 should have overwritten W1's work!", "v3", cache.get(key));
         assertNoLocks();
      } else {
         Collection<Exception> combined = new HashSet<Exception>(w1exceptions);
         combined.addAll(w2exceptions);
         assertFalse("Exceptions are expected!", combined.isEmpty());
         assertEquals("Expects one exception.", 1, combined.size());
         assertTrue("Wrong exception type.", combined.iterator().next() instanceof RollbackException);
      }
   }

   private void throwExceptions(Collection<Exception> exceptions) throws Exception {
      Iterator<Exception> iterator = exceptions.iterator();
      if (iterator.hasNext()) {
         throw iterator.next();
      }
   }

   protected class EntryWriter implements Callable<Void> {
      private final CyclicBarrier barrier;

      public EntryWriter(CyclicBarrier barrier) {
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
               if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
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
