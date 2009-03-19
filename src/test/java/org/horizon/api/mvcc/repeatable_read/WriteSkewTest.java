package org.horizon.api.mvcc.repeatable_read;

import org.horizon.Cache;
import org.horizon.api.mvcc.LockAssert;
import org.horizon.config.Configuration;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.lock.IsolationLevel;
import org.horizon.lock.LockManager;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@Test(groups = {"functional", "mvcc"}, testName = "api.mvcc.repeatable_read.WriteSkewTest")
public class WriteSkewTest {
   protected TransactionManager tm;
   protected LockManager lockManager;
   protected InvocationContextContainer icc;
   protected CacheManager cacheManager;
   protected Cache cache;

   @BeforeTest
   public void setUp() {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      c.setLockAcquisitionTimeout(200);
      c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);

      cacheManager = new DefaultCacheManager(c);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   private void postStart() {
      lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      icc = TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class);
      tm = TestingUtil.extractComponentRegistry(cache).getComponent(TransactionManager.class);
   }

   protected void assertNoLocks() {
      LockAssert.assertNoLocks(lockManager, icc);
   }

   public void testDontCheckWriteSkew() throws Exception {
      Configuration noWriteSkewCheck = new Configuration();
      noWriteSkewCheck.setWriteSkewCheck(false);
      cacheManager.defineCache("noWriteSkewCheck", noWriteSkewCheck);
      cache = cacheManager.getCache("noWriteSkewCheck");
      postStart();
      doTest(true);
   }

   public void testCheckWriteSkew() throws Exception {
      Configuration writeSkewCheck = new Configuration();
      writeSkewCheck.setWriteSkewCheck(true);
      cacheManager.defineCache("writeSkewCheck", writeSkewCheck);
      cache = cacheManager.getCache("writeSkewCheck");
      postStart();
      doTest(false);
   }

   private void doTest(final boolean allowWriteSkew) throws Exception {
      cache.put("k", "v");
      final Set<Exception> w1exceptions = new HashSet<Exception>();
      final Set<Exception> w2exceptions = new HashSet<Exception>();
      final CountDownLatch w1Signal = new CountDownLatch(1);
      final CountDownLatch w2Signal = new CountDownLatch(1);
      final CountDownLatch threadSignal = new CountDownLatch(2);

      Thread w1 = new Thread("Writer-1") {
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assert "v".equals(cache.get("k"));
               threadSignal.countDown();
               didCoundDown = true;
               w1Signal.await();
               cache.put("k", "v2");
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

      Thread w2 = new Thread("Writer-2") {
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assert "v".equals(cache.get("k"));
               threadSignal.countDown();
               didCoundDown = true;
               w2Signal.await();
               cache.put("k", "v3");
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
         throwExceptions(w1exceptions, w2exceptions);
         assert w2exceptions.size() == 0;
         assert w1exceptions.size() == 0;
         assert "v3".equals(cache.get("k")) : "W2 should have overwritten W1's work!";

         assertNoLocks();
      }
   }

   private void throwExceptions(Collection<Exception>... exceptions) throws Exception {
      for (Collection<Exception> ce : exceptions) {
         for (Exception e : ce) throw e;
      }
   }
}
