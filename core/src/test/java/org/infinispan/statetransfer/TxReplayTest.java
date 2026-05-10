package org.infinispan.statetransfer;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

import jakarta.transaction.Status;

/**
 * Tests the prepare replay.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.TxReplayTest")
public class TxReplayTest extends MultipleCacheManagersTest {

   private static final String VALUE = "value";

   public void testReplay() throws Exception {
      assertClusterSize("Wrong cluster size", 3);
      final Object key = new MagicKey(cache(0), cache(1));
      final Cache<Object, Object> newBackupOwnerCache = cache(2);
      final TxCommandInterceptor interceptor = TxCommandInterceptor.inject(newBackupOwnerCache);

      EmbeddedTransactionManager transactionManager = (EmbeddedTransactionManager) tm(0);
      transactionManager.begin();
      cache(0).put(key, VALUE);
      final EmbeddedTransaction transaction = transactionManager.getTransaction();
      transaction.runPrepare();
      assertEquals(Status.STATUS_PREPARED, transaction.getStatus(), "Wrong transaction status before killing backup owner.");

      //now, we kill cache(1). the transaction is prepared in cache(1) and it should be forward to cache(2)
      killMember(1);

      checkIfTransactionExists(newBackupOwnerCache);
      assertEquals(Status.STATUS_PREPARED, transaction.getStatus(), "Wrong transaction status after killing backup owner.");
      transaction.runCommit(false);

      assertNoTransactions();

      assertEquals(1, interceptor.numberPrepares.get(), "Wrong number of prepares!");
      assertEquals(1, interceptor.numberCommits.get(), "Wrong number of commits!");
      assertEquals(0, interceptor.numberRollbacks.get(), "Wrong number of rollbacks!");

      checkKeyInDataContainer(key);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .useSynchronization(false)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .recovery().disable();
      builder.clustering()
            .hash().numOwners(2)
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(3, TestDataSCI.INSTANCE, builder);
   }

   private void checkKeyInDataContainer(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         DataContainer container = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry entry = container.peek(key);
         assertNotNull(entry, "Cache '" + address(cache) + "' does not contain key!");
         assertEquals(VALUE, entry.getValue(), "Cache '" + address(cache) + "' has wrong value!");
      }
   }

   private void checkIfTransactionExists(Cache<Object, Object> cache) {
      TransactionTable table = TestingUtil.extractComponent(cache, TransactionTable.class);
      assertFalse(table.getRemoteTransactions().isEmpty(), "Expected a remote transaction.");
   }

   static class TxCommandInterceptor extends DDAsyncInterceptor {
      //counters
      private final AtomicInteger numberPrepares = new AtomicInteger(0);
      private final AtomicInteger numberCommits = new AtomicInteger(0);
      private final AtomicInteger numberRollbacks = new AtomicInteger(0);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberPrepares.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberCommits.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberRollbacks.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      public static TxCommandInterceptor inject(Cache cache) {
         AsyncInterceptorChain chain = extractInterceptorChain(cache);
         if (chain.containsInterceptorType(TxCommandInterceptor.class)) {
            return chain.findInterceptorWithClass(TxCommandInterceptor.class);
         }
         TxCommandInterceptor interceptor = new TxCommandInterceptor();
         chain.addInterceptorBefore(interceptor, CallInterceptor.class);
         return interceptor;
      }
   }
}
