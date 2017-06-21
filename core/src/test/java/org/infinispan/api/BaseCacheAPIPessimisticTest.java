package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.TransactionManager;

import org.infinispan.LockedStream;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.1
 */
public abstract class BaseCacheAPIPessimisticTest extends CacheAPITest {
   @Override
   protected void amend(ConfigurationBuilder cb) {
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
   }

   /**
    * Tests to make sure that locked stream works properly when another user has the lock for a given key
    */
   public void testLockedStreamBlocked() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      for (int i = 0; i < 10; i++) {
         cache.put(i, "value" + i);
      }

      CyclicBarrier barrier = new CyclicBarrier(2);

      int key = 4;

      Future<Object> putFuture = fork(() -> TestingUtil.withTx(cache.getAdvancedCache().getTransactionManager(), () -> {
         Object prev = cache.put(key, "value" + key + "-new");
         // Wait for main thread to get to same point
         barrier.await(10, TimeUnit.SECONDS);
         // Main thread lets us complete
         barrier.await(10, TimeUnit.SECONDS);
         return prev;
      }));

      // Wait until fork thread has alredy locked the key
      barrier.await(10, TimeUnit.SECONDS);

      LockedStream<Object, Object> stream = cache.getAdvancedCache().lockedStream();
      Future<?> forEachFuture = fork(() -> stream.filter(e -> e.getKey().equals(key)).forEach((c, e) ->
            assertEquals("value" + key + "-new", c.put(e.getKey(), String.valueOf(e.getValue() + "-other")))));

      Exceptions.expectException(TimeoutException.class, () -> forEachFuture.get(50, TimeUnit.MILLISECONDS));

      // Let the tx put complete
      barrier.await(10, TimeUnit.SECONDS);

      forEachFuture.get(10, TimeUnit.MINUTES);

      // The put should replace the value that forEach inserted
      assertEquals("value" + key, putFuture.get(10, TimeUnit.SECONDS));
      // The put should be last since it had to wait until lock was released on forEachWithLock
      assertEquals("value" + key + "-new-other", cache.get(key));

      // Make sure the locks were cleaned up properly
      LockManager lockManager = cache.getAdvancedCache().getComponentRegistry().getComponent(LockManager.class);
      assertEquals(0, lockManager.getNumberOfLocksHeld());
   }

   @DataProvider(name = "testLockedStreamInTx")
   public Object[][] testLockedStreamInTxProvider() {
      return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE} };
   }

   @Test(dataProvider = "testLockedStreamInTx")
   public void testLockedStreamInTxCommit(Boolean shouldCommit) throws Exception {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();

      TestingUtil.withTx(tm, () -> {
         cache.getAdvancedCache().lockedStream().forEach((c, e) -> c.put(e.getKey(), e.getValue() + "-changed"));
         if (!shouldCommit) tm.setRollbackOnly();
         return null;
      });

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + "-changed", cache.get(i));
      }
   }

   public void testLockedStreamTxInsideConsumer() {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      cache.getAdvancedCache().lockedStream().forEach((c, e) -> {
         try {
            TestingUtil.withTx(c.getAdvancedCache().getTransactionManager(), () -> c.put(e.getKey(), e.getValue() + "-changed"));
         } catch (Exception e1) {
            throw new RuntimeException(e1);
         }
      });

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + "-changed", cache.get(i));
      }
   }

   @DataProvider(name = "testLockedStreamInTxAndConsumer")
   public Object[][] testLockedStreamInTxAndConsumerProvider() {
      return new Object[][] {
         { Boolean.TRUE, Boolean.TRUE },
         { Boolean.TRUE, Boolean.FALSE },
         { Boolean.FALSE, Boolean.TRUE},
         { Boolean.FALSE, Boolean.FALSE}
      };
   }

   @Test(dataProvider = "testLockedStreamInTxAndConsumer")
   public void testLockedStreamInTxAndConsumer(Boolean outerCommit, Boolean innerCommit) throws Exception {
      for (int i = 0; i < 5; i++) {
         cache.put(i, "value" + i);
      }

      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();

      TestingUtil.withTx(tm, () -> {
         cache.getAdvancedCache().lockedStream().forEach((c, e) -> {
            try {
               TransactionManager innerTm = c.getAdvancedCache().getTransactionManager();
               TestingUtil.withTx(innerTm, () -> {
                  c.put(e.getKey(), e.getValue() + "-changed");
                  if (!innerCommit) innerTm.setRollbackOnly();
                  return null;
               });
            } catch (Exception e1) {
               throw new RuntimeException(e1);
            }
         });
         if (!outerCommit) tm.setRollbackOnly();
         return null;
      });

      for (int i = 0; i < 5; i++) {
         assertEquals("value" + i + (innerCommit ? "-changed" : ""), cache.get(i));
      }
   }
}
