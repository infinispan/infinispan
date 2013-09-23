package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

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


   private TransactionTable txTable() {
      return TestingUtil.getTransactionTable(cache);
   }
}
