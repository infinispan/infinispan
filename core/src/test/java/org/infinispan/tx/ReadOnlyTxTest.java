package org.infinispan.tx;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.ReadOnlyTxTest")
@CleanupAfterMethod
public class ReadOnlyTxTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      configuration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      configure(configuration);
      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   protected void configure(ConfigurationBuilder builder) {
      builder.transaction().useSynchronization(false);
   }

   public void testSimpleReadOnlTx() throws Exception {
      tm().begin();
      assert cache.get("k") == null;
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && localTransaction.isReadOnly();
   }

   public void testNotROWhenHasWrites() throws Exception {
      tm().begin();
      cache.put("k", "v");
      assert TestingUtil.extractLockManager(cache).isLocked("k");
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && !localTransaction.isReadOnly();
   }

   public void testROWhenHasOnlyLocksAndReleasedProperly() throws Exception {
      cache.put("k", "v");
      tm().begin();
      cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get("k");
      assert TestingUtil.extractLockManager(cache).isLocked("k");
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && localTransaction.isReadOnly();

      tm().resume(transaction);

      tm().commit();

      assert !TestingUtil.extractLockManager(cache).isLocked("k");
   }

   public void testSingleCommitCommand() throws Exception {
      cache.put("k", "v");
      CommitCommandCounterInterceptor counterInterceptor = new CommitCommandCounterInterceptor();
      cache.getAdvancedCache().addInterceptor(counterInterceptor, 0);
      try {
         tm().begin();
         AssertJUnit.assertEquals("Wrong value for key k.", "v", cache.get("k"));
         tm().commit();
      } finally {
         cache.getAdvancedCache().getAdvancedCache().removeInterceptor(counterInterceptor.getClass());
      }
      AssertJUnit.assertEquals("Wrong number of CommitCommand.", numberCommitCommand(), counterInterceptor.counter.get());
   }

   protected int numberCommitCommand() {
      //in this case, the transactions are committed in 1 phase due to pessimistic locking.
      return 0;
   }

   private TransactionTable txTable() {
      return TestingUtil.getTransactionTable(cache);
   }

   private class CommitCommandCounterInterceptor extends BaseCustomInterceptor {

      public final AtomicInteger counter = new AtomicInteger(0);

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         counter.incrementAndGet();
         return invokeNextInterceptor(ctx, command);
      }
   }
}
