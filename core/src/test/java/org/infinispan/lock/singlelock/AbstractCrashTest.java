package org.infinispan.lock.singlelock;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractCrashTest extends MultipleCacheManagersTest {

   protected CacheMode cacheMode;
   protected LockingMode lockingMode;
   protected Boolean useSynchronization;

   protected AbstractCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      this.cacheMode = cacheMode;
      this.lockingMode = lockingMode;
      this.useSynchronization = useSynchronization;
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(TestDataSCI.INSTANCE, c, 3);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, true);
      c.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .useSynchronization(useSynchronization)
            .lockingMode(lockingMode)
            .recovery().disable()
            .clustering()
            .l1().disable()
            .hash().numOwners(3)
            .stateTransfer().fetchInMemoryState(false)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return c;
   }

   protected Future<Void> beginAndPrepareTx(final Object k, final int cacheIndex) {
      return fork(() -> {
         try {
            tm(cacheIndex).begin();
            cache(cacheIndex).put(k,"v");
            final EmbeddedTransaction transaction = (EmbeddedTransaction) tm(cacheIndex).getTransaction();
            transaction.runPrepare();
         } catch (Throwable e) {
            log.errorf(e, "Error preparing transaction for key %s on cache %s", k, cache(cacheIndex));
         }
      });
   }

   protected Future<Void> beginAndCommitTx(final Object k, final int cacheIndex) {
      return fork(() -> {
         try {
            tm(cacheIndex).begin();
            cache(cacheIndex).put(k, "v");
            tm(cacheIndex).commit();
         } catch (Throwable e) {
            log.errorf(e, "Error committing transaction for key %s on cache %s", k, cache(cacheIndex));
         }
      });
   }

   public static class TxControlInterceptor extends DDAsyncInterceptor {

      public CountDownLatch prepareProgress = new CountDownLatch(1);
      public CountDownLatch preparedReceived = new CountDownLatch(1);
      public CountDownLatch commitReceived = new CountDownLatch(1);
      public CountDownLatch commitProgress = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            preparedReceived.countDown();
            prepareProgress.await();
         });
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitReceived.countDown();
         commitProgress.await();
         return invokeNext(ctx, command);

      }
   }

   protected void skipTxCompletion(final AdvancedCache<Object, Object> cache, final CountDownLatch releaseLocksLatch) {
      RpcManager rpcManager = new AbstractDelegatingRpcManager(cache.getRpcManager()) {
         @Override
         protected <T> void performSend(Collection<Address> targets, ReplicableCommand command,
                                        Function<ResponseCollector<T>, CompletionStage<T>> invoker) {
            if (command instanceof TxCompletionNotificationCommand) {
               releaseLocksLatch.countDown();
               log.tracef("Skipping TxCompletionNotificationCommand");
            } else {
               super.performSend(targets, command, invoker);
            }
         }
      };

      //not too nice: replace the rpc manager in the class that builds the Sync objects
      final TransactionTable transactionTable = TestingUtil.getTransactionTable(cache(1));
      TestingUtil.replaceField(rpcManager, "rpcManager", transactionTable, TransactionTable.class);

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      txControlInterceptor.prepareProgress.countDown();
      txControlInterceptor.commitProgress.countDown();
      extractInterceptorChain(advancedCache(1)).addInterceptor(txControlInterceptor, 1);
   }

}
