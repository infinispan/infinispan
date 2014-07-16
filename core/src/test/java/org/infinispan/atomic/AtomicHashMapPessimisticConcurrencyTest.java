package org.infinispan.atomic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.ValueFuture;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;


/**
 * Test class for AtomicHashMap.
 *
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "atomic.AtomicHashMapPessimisticConcurrencyTest")
public class AtomicHashMapPessimisticConcurrencyTest extends SingleCacheManagerTest {
   private static final Log log = LogFactory.getLog(AtomicHashMapPessimisticConcurrencyTest.class);

   public static final String KEY = "key";
   private LockingMode lockingMode = LockingMode.PESSIMISTIC;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.locking().lockAcquisitionTimeout(1000);
      builder.invocationBatching().enable();
      builder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup()).lockingMode(lockingMode);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testConcurrentCreate() throws Exception {
      tm().begin();
      AtomicMapLookup.getAtomicMap(cache, KEY);

      final AtomicBoolean gotTimeoutException = new AtomicBoolean();
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm().begin();

               try {
                  AtomicMapLookup.getAtomicMap(cache, KEY);
               } catch (TimeoutException e) {
                  // this is the exception we were expecting
                  gotTimeoutException.set(true);
               } finally {
                  tm().rollback();
               }
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }).get(10, TimeUnit.SECONDS);

      assert gotTimeoutException.get();
   }

   public void testLockTimeout() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm().begin();
      atomicMap.put(1, "");

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            tm().begin();

            try {
               AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);
               otMap.put(1, "val");
            } finally {
               tm().rollback();
            }
            return null;
         }
      });

      try {
         future.get(10, TimeUnit.SECONDS);
         fail("Should have failed with a TimeoutException");
      } catch (ExecutionException e) {
         assertTrue(e.getCause() instanceof TimeoutException);
      }
   }

   public void testConcurrentPut() throws Exception {
      final CountDownLatch readLatch = new CountDownLatch(1);
      final CountDownLatch commitLatch = new CountDownLatch(1);

      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm().begin();
      atomicMap.put(1, "value1");

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               tm().begin();
               AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);

               assertEquals(0, otMap.size());
               readLatch.countDown();

               otMap.put(2, "value2");
               commitLatch.await(10, TimeUnit.SECONDS);

               tm().commit();
            } catch (Exception e) {
               tm().rollback();
               throw e;
            }
            return null;
         }
      });

      readLatch.await(10, TimeUnit.SECONDS);
      tm().commit();
      commitLatch.countDown();

      future.get(10, TimeUnit.SECONDS);
      assertEquals(new HashSet<Integer>(Arrays.asList(1, 2)), atomicMap.keySet());
   }

   public void testConcurrentRemove() throws Exception {
      final CountDownLatch readLatch = new CountDownLatch(1);
      final CountDownLatch commitLatch = new CountDownLatch(1);

      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm().begin();
      atomicMap.put(1, "value1");
      atomicMap.put(2, "value2");
      atomicMap.put(3, "value3");
      tm().commit();

      tm().begin();
      atomicMap.remove(1);

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               tm().begin();
               AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);

               assertEquals(3, otMap.size());
               readLatch.countDown();

               otMap.remove(2);
               commitLatch.await(10, TimeUnit.SECONDS);

               tm().commit();
            } catch (Exception e) {
               tm().rollback();
               throw e;
            }
            return null;
         }
      });

      readLatch.await(10, TimeUnit.SECONDS);
      tm().commit();
      commitLatch.countDown();

      future.get(10, TimeUnit.SECONDS);
      assertEquals(new HashSet<Integer>(Arrays.asList(3)), atomicMap.keySet());
   }

   public void testReadAfterTxStarted() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      atomicMap.put(1, "existing");
      tm().begin();
      atomicMap.put(1, "newVal");

      final ValueFuture responseBeforeCommit = new ValueFuture();
      final ValueFuture responseAfterCommit = new ValueFuture();
      final CountDownLatch commitLatch = new CountDownLatch(1);

      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            tm().begin();

            try {
               AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);

               responseBeforeCommit.set(otMap.get(1));

               // wait until the main thread commits the transaction
               commitLatch.await();

               responseAfterCommit.set(otMap.get(1));
            } finally {
               tm().rollback();
            }
            return null;
         }
      });

      assertEquals("existing", responseBeforeCommit.get());

      tm().commit();
      commitLatch.countDown();

      future.get(10, TimeUnit.SECONDS);
      assertEquals("newVal", atomicMap.get(1));
      assertEquals("newVal", responseAfterCommit.get());
   }
}
