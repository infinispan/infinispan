package org.infinispan.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.InvalidTransactionException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;
import org.infinispan.transaction.tm.EmbeddedBaseTransactionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Reproducer for ISPN-12798
 * <p>
 * The issue is the WF {@link TransactionManager#resume(Transaction)} is invoked with the ApplyStateTransaction. The WF
 * {@link TransactionManager} doesn't know the instance and fails with {@link InvalidTransactionException}.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Test(groups = "functional", testName = "tx.StateTransferTransactionTest")
public class StateTransferTransactionTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(StateTransferTransactionTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(2);
   }

   @DataProvider(name = "data")
   public static Object[][] configurationFiles() {
      return new Object[][]{
            {true, true},
            {true, false},
            {false, false},
      };
   }

   @Test(dataProvider = "data")
   public void testStateTransferTransactionNotEnlisted(boolean useSync, boolean useRegistry) {
      final String cacheName = String.format("cache-%s", suffix(useSync, useRegistry));
      final String key = String.format("key-%s", suffix(useSync, useRegistry));

      log.debugf("Starting cache in node0");
      manager(0).defineConfiguration(cacheName, configurationBuilder(useSync, useRegistry).build());
      Cache<String, String> cache0 = manager(0).getCache(cacheName);

      cache0.put(key, "value");

      log.debugf("Starting cache in node1");
      manager(1).defineConfiguration(cacheName, configurationBuilder(useSync, useRegistry).build());
      Cache<String, String> cache1 = manager(1).getCache(cacheName);

      waitForClusterToForm(cacheName);

      CollectTxInterceptor interceptor = cache1.getAdvancedCache().getAsyncInterceptorChain().findInterceptorWithClass(CollectTxInterceptor.class);
      assertEquals(1, interceptor.stateTransferTransactions.size());
      TransactionImpl stateTransferTx = interceptor.stateTransferTransactions.iterator().next();

      //useSync=false will fail here because Infinispan invokes ApplyStateTransaction.enlistResource() directly
      assertTrue("Found XaResource", stateTransferTx.getEnlistedResources().isEmpty());
      //useSync=true & useRegistry=false fails here because Infinispan invokes ApplyStateTransaction.registerSynchronization() directly
      assertTrue("Found Synchronization", stateTransferTx.getEnlistedSynchronization().isEmpty());
      //useSync=true & useRegistry=false fails here. DummyTransactionManager.resume() throws an exception which fails the state transfer
      //it simulates the WF environment.
      assertEquals("Wrong value in cache1", "value", cache1.get(key));
   }

   private static String suffix(boolean useSync, boolean useRegistry) {
      return String.format("%s-%s", useSync ? "sync" : "xa", useRegistry ? "registry" : "no-registry");
   }

   private static ConfigurationBuilder configurationBuilder(boolean useSync, boolean useRegistry) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .useSynchronization(useSync)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
      if (useRegistry) {
         builder.transaction().transactionSynchronizationRegistryLookup(new DummyTransactionSynchronizationRegistryLookup());
      }
      builder.customInterceptors().addInterceptor().interceptor(new CollectTxInterceptor()).after(TxInterceptor.class);
      builder.clustering().hash().numSegments(1);
      return builder;
   }

   private static boolean isStateTransferTransaction(Transaction tx) {
      return tx instanceof TransactionImpl && ((TransactionImpl) tx).getXid().getFormatId() == 2;
   }

   static class CollectTxInterceptor extends BaseCustomAsyncInterceptor {

      private final Set<TransactionImpl> stateTransferTransactions;

      CollectTxInterceptor() {
         stateTransferTransactions = Collections.synchronizedSet(new HashSet<>());
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (ctx.isInTxScope() && ctx.isOriginLocal()) {

            LocalTransaction localTx = ((LocalTxInvocationContext) ctx).getCacheTransaction();
            Transaction tx = localTx.getTransaction();
            if (isStateTransferTransaction(tx)) {
               log.debugf("collect transaction %s. list=%s", tx, stateTransferTransactions);
               stateTransferTransactions.add((TransactionImpl) tx);
            }
         }
         return super.handleDefault(ctx, command);
      }
   }

   static class DummyTransactionManager extends EmbeddedBaseTransactionManager {

      @Override
      public void resume(Transaction tx) throws InvalidTransactionException, IllegalStateException, SystemException {
         if (isStateTransferTransaction(tx)) {
            log.debugf("Resume invoked with invalid transaction %s", tx);
            //simulates WF exception
            throw new InvalidTransactionException("Transaction is not a supported instance");
         }
         super.resume(tx);
      }
   }

   static class DummyTransactionManagerLookup implements TransactionManagerLookup {

      private static final DummyTransactionManager INSTANCE = new DummyTransactionManager();

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return INSTANCE;
      }
   }

   static class DummyTransactionSynchronizationRegistry implements TransactionSynchronizationRegistry {

      @Override
      public Object getTransactionKey() {
         return null;
      }

      @Override
      public int getTransactionStatus() {
         return 0;
      }

      @Override
      public boolean getRollbackOnly() throws IllegalStateException {
         return false;
      }

      @Override
      public void setRollbackOnly() throws IllegalStateException {

      }

      @Override
      public void registerInterposedSynchronization(Synchronization synchronization) throws IllegalStateException {
         Transaction tx = DummyTransactionManagerLookup.INSTANCE.getTransaction();
         if (tx == null) {
            throw new IllegalStateException();
         }
         try {
            tx.registerSynchronization(synchronization);
         } catch (RollbackException | SystemException e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public Object getResource(Object o) throws IllegalStateException {
         return null;
      }

      @Override
      public void putResource(Object o, Object o1) throws IllegalStateException {

      }
   }

   static class DummyTransactionSynchronizationRegistryLookup implements TransactionSynchronizationRegistryLookup {

      private static final DummyTransactionSynchronizationRegistry INSTANCE = new DummyTransactionSynchronizationRegistry();

      @Override
      public TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
         return INSTANCE;
      }
   }
}
