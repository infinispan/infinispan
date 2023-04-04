package org.infinispan.tx.locking;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.TransactionSynchronizerInterceptor;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Reproducer for ISPN-13024
 * <p>
 * Note: remote transactions are synchronized in {@link TransactionSynchronizerInterceptor}, so you cannot have a
 * {@link RollbackCommand} executing while waiting for any lock.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Test(groups = "functional", testName = "tx.locking.RollbackDuringLockAcquisitionTest")
public class RollbackDuringLockAcquisitionTest extends SingleCacheManagerTest {

   private static String concat(Object... components) {
      return Arrays.stream(components)
                   .map(String::valueOf)
                   .map(String::toLowerCase)
                   .collect(Collectors.joining("-"));
   }

   @DataProvider(name = "sync-tm")
   public Object[][] transactionManagerLookup() {
      //use sync & tm lookup instance
      return new Object[][]{
            {true, new EmbeddedTransactionManagerLookup()},
            {true, new JBossStandaloneJTAManagerLookup()},
            {false, new EmbeddedTransactionManagerLookup()},
            {false, new JBossStandaloneJTAManagerLookup()}
      };
   }

   /*
    * The "real world" conditions are:
    *  - Narayana timeout <= lock timeout
    *
    * Cause/Steps:
    *  - Narayana aborts a transaction which is waiting for a lock "L"
    *  - The RollbackCommand is sent.
    *    - Lock "L" is not locked by the transaction, so unlockAll(context.getLockedKeys(), context.getLockOwner());
    *      never changes the state from WAITING => RELEASED
    *  - When the lock "L" is released, the state changes from WAITING => ACQUIRED
    *  - No RollbackCommand/CommitCommand is followed leaving the lock "L" in ACQUIRED state forever
    */
   @Test(dataProvider = "sync-tm")
   public void testRollbackWhileWaitingForLockDuringPut(boolean useSynchronization, TransactionManagerLookup lookup)
         throws Exception {
      final String cacheName = concat("local-put", lookup.getClass().getSimpleName(), useSynchronization);
      final String key = concat("reaper-put", lookup.getClass().getSimpleName(), useSynchronization);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction()
             .transactionManagerLookup(lookup)
             .transactionMode(TransactionMode.TRANSACTIONAL)
             .lockingMode(LockingMode.PESSIMISTIC)
             .useSynchronization(useSynchronization)
             .recovery().disable();
      builder.locking()
             .lockAcquisitionTimeout(TimeUnit.MINUTES.toMillis(1));

      cacheManager.defineConfiguration(cacheName, builder.build());
      Cache<String, String> cache = cacheManager.getCache(cacheName);
      LockManager lockManager = extractLockManager(cache);
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();

      // simulates lock acquired by other transaction
      lockManager.lock(key, "_tx_", 1, TimeUnit.SECONDS).lock();
      assertLocked(cache, key);

      tm.begin();
      CompletableFuture<?> put = cache.putAsync(key, "value1");

      GlobalTransaction gtx = extractComponent(cache, TransactionTable.class).getGlobalTransaction(tm.getTransaction());

      // wait until the tx is queued in the InfinispanLock
      eventually(() -> lockManager.getLock(key).containsLockOwner(gtx));

      //abort the transaction, simulates the TM's reaper aborting a long running transaction
      tm.rollback();

      // release the lock. the "put" must not acquire the lock
      lockManager.unlock(key, "_tx_");

      Exceptions.expectCompletionException(InvalidTransactionException.class, put);

      assertNotLocked(cache, key);
      assertNoTransactions(cache);
   }

   @Test(dataProvider = "sync-tm")
   public void testRollbackWhileWaitingForLockDuringLock(boolean useSynchronization, TransactionManagerLookup lookup)
         throws Exception {
      final String cacheName = concat("local-lock", lookup.getClass().getSimpleName(), useSynchronization);
      final String key = concat("reaper-lock", lookup.getClass().getSimpleName(), useSynchronization);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction()
             .transactionManagerLookup(lookup)
             .transactionMode(TransactionMode.TRANSACTIONAL)
             .lockingMode(LockingMode.PESSIMISTIC)
             .useSynchronization(useSynchronization)
             .recovery().disable();
      builder.locking()
             .lockAcquisitionTimeout(TimeUnit.MINUTES.toMillis(1));

      cacheManager.defineConfiguration(cacheName, builder.build());
      Cache<String, String> cache = cacheManager.getCache(cacheName);
      LockManager lockManager = extractLockManager(cache);
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();

      // simulates lock acquired by other transaction
      lockManager.lock(key, "_tx_", 1, TimeUnit.SECONDS).lock();
      assertLocked(cache, key);

      AtomicReference<Transaction> tx = new AtomicReference<>();

      Future<Boolean> lock = fork(() -> {
         tm.begin();
         tx.set(tm.getTransaction());
         try {
            return cache.getAdvancedCache().lock(key);
         } finally {
            // ignore transaction from rollback.
            // if the tests works as expected, the transaction is rolled back when it reaches this point and the rollback()
            //    method will throw an IllegalStateException (which we don't want for the test)
            safeRollback(tm);
         }
      });

      TransactionTable txTable = extractComponent(cache, TransactionTable.class);
      eventually(() -> !txTable.getLocalGlobalTransaction().isEmpty());

      assertEquals(1, txTable.getLocalGlobalTransaction().size());
      GlobalTransaction gtx = txTable.getLocalGlobalTransaction().iterator().next();

      eventually(() -> lockManager.getLock(key).containsLockOwner(gtx));

      //abort the transaction, simulates the TM's reaper aborting a long running transaction
      tx.get().rollback();

      // release the lock. the "lock" must not acquire the lock
      lockManager.unlock(key, "_tx_");

      Exceptions.expectException(ExecutionException.class, InvalidTransactionException.class, lock::get);
      assertNotLocked(cache, key);
      assertNoTransactions(cache);
   }


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

}
