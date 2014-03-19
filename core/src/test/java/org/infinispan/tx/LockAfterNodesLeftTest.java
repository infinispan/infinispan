
package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * Test for ISPN-2469.
 *
 * @author Carsten Lohmann
 */
@Test(groups = "functional", testName = "tx.LockAfterNodesLeftTest")
public class LockAfterNodesLeftTest extends MultipleCacheManagersTest {

   private final int INITIAL_CLUSTER_SIZE = 6;
   private final int NUM_NODES_TO_STOP_FOR_TEST = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cacheConfig.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createClusteredCaches(INITIAL_CLUSTER_SIZE, cacheConfig);
      waitForClusterToForm();
   }

   public void test() throws Exception {
      log.debug("Adding test key");
      cache(0).put("k", "v");

      // ensure that there are no transactions left
      for (int i = 0; i < INITIAL_CLUSTER_SIZE; i++) {
         final TransactionTable transactionTable = TestingUtil.getTransactionTable(cache(i));

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return transactionTable.getLocalTransactions().isEmpty();
            }
         });

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return transactionTable.getRemoteTransactions().isEmpty();
            }
         });
      }

      TestingUtil.sleepThread(2000);

      log.debug("Shutting down some nodes ..");
      for (int i = 0; i < NUM_NODES_TO_STOP_FOR_TEST; i++) {
         cacheManagers.get(INITIAL_CLUSTER_SIZE - 1 - i).stop();
      }
      log.debug("Shutdown completed");
      final int remainingNodesCount = INITIAL_CLUSTER_SIZE - NUM_NODES_TO_STOP_FOR_TEST;

      TestingUtil.sleepThread(2000);

      // now do a parallel put on the cache
      final String key = "key";
      final AtomicInteger errorCount = new AtomicInteger();
      final AtomicInteger rolledBack = new AtomicInteger();

      final CountDownLatch latch = new CountDownLatch(1);
      Thread[] threads = new Thread[remainingNodesCount];
      for (int i = 0; i < remainingNodesCount; i++) {
         final int nodeIndex = i;
         threads[i] = new Thread("LockAfterNodesLeftTest.Putter-" + i) {
            public void run() {
               try {
                  latch.await();

                  log.debug("about to begin transaction...");
                  tm(nodeIndex).begin();
                  try {
                     log.debug("Getting lock on cache key");
                     cache(nodeIndex).getAdvancedCache().lock(key);
                     log.debug("Got lock");
                     cache(nodeIndex).put(key, "value");

                     log.debug("Done with put");
                     TestingUtil.sleepRandom(200);
                     tm(nodeIndex).commit();
                  } catch (Throwable e) {
                     if (e instanceof RollbackException) {
                        rolledBack.incrementAndGet();
                     } else if (tm(nodeIndex).getTransaction() != null) {
                        // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                        try {
                           tm(nodeIndex).rollback();
                           rolledBack.incrementAndGet();
                        } catch (SystemException e1) {
                           log.error("Failed to rollback", e1);
                        }
                     }
                     throw e;
                  }
               } catch (Throwable e) {
                  errorCount.incrementAndGet();
                  log.error(e);
               }
            }
         };
         threads[i].start();
      }

      latch.countDown();
      for (Thread t : threads) {
         t.join();
      }

      log.trace("Got errors: " + errorCount.get());
      assertEquals(0, errorCount.get());
   }
}
