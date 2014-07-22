package org.infinispan.lock.singlelock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.dld.ControlledRpcManager;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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
      createCluster(c, 3);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, true);
      c.transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .useSynchronization(useSynchronization)
            .lockingMode(lockingMode)
            .recovery().disable()
            .clustering()
            .l1().disable()
            .hash().numOwners(3)
            .stateTransfer().fetchInMemoryState(false);
      return c;
   }

   protected Object beginAndPrepareTx(final Object k, final int cacheIndex) {
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(cacheIndex).begin();
               cache(cacheIndex).put(k,"v");
               final DummyTransaction transaction = (DummyTransaction) tm(cacheIndex).getTransaction();
               transaction.runPrepare();
            } catch (Throwable e) {
               log.errorf(e, "Error preparing transaction for key %s on cache %s", k, cache(cacheIndex));
            }
         }
      });
      return k;
   }

   protected Object beginAndCommitTx(final Object k, final int cacheIndex) {
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(cacheIndex).begin();
               cache(cacheIndex).put(k, "v");
               tm(cacheIndex).commit();
            } catch (Throwable e) {
               log.errorf(e, "Error committing transaction for key %s on cache %s", k, cache(cacheIndex));
            }
         }
      });
      return k;
   }

   public static class TxControlInterceptor extends CommandInterceptor {

      public CountDownLatch prepareProgress = new CountDownLatch(1);
      public CountDownLatch preparedReceived = new CountDownLatch(1);
      public CountDownLatch commitReceived = new CountDownLatch(1);
      public CountDownLatch commitProgress = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         final Object result = super.visitPrepareCommand(ctx, command);
         preparedReceived.countDown();
         prepareProgress.await();
         return result;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitReceived.countDown();
         commitProgress.await();
         return super.visitCommitCommand(ctx, command);

      }
   }

   protected void prepareCache(final CountDownLatch releaseLocksLatch) {
      RpcManager rpcManager = new ControlledRpcManager(advancedCache(1).getRpcManager()) {
         @Override
         public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc,
                                                      RpcOptions options) {
            if (rpc instanceof TxCompletionNotificationCommand) {
               releaseLocksLatch.countDown();
               return null;
            } else {
               return realOne.invokeRemotely(recipients, rpc, options);
            }
         }
      };

      //not too nice: replace the rpc manager in the class that builds the Sync objects
      final TransactionTable transactionTable = TestingUtil.getTransactionTable(cache(1));
      TestingUtil.replaceField(rpcManager, "rpcManager", transactionTable, TransactionTable.class);

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      txControlInterceptor.prepareProgress.countDown();
      txControlInterceptor.commitProgress.countDown();
      advancedCache(1).addInterceptor(txControlInterceptor, 1);
   }

}
