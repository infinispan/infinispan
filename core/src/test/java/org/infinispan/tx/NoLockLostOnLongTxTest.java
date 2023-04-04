package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import jakarta.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CheckTransactionRpcCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests if long running transactions from members which are alive, are not rolled-back by mistake.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "functional", testName = "tx.NoLockLostOnLongTxTest")
public class NoLockLostOnLongTxTest extends MultipleCacheManagersTest {

   private static final long COMPLETED_TX_TIMEOUT = 10000;
   private ControlledTimeService timeService;

   private static Method extractCleanupMethod() throws NoSuchMethodException {
      Method m = TransactionTable.class.getDeclaredMethod("cleanupTimedOutTransactions", (Class<?>[]) null);
      m.setAccessible(true);
      return m;
   }

   @DataProvider(name = "long-tx-test")
   public static Object[][] longTxDataProvider() {
      return new Object[][]{
            {TestLockMode.PESSIMISTIC},
            {TestLockMode.OPTIMISTIC}
      };
   }

   @Test(dataProvider = "long-tx-test")
   public void testLongTx(LongTxTestParameter testParameter) throws Exception {
      String cacheName = testParameter.cacheName();
      defineConfigurationOnAllManagers(cacheName, testParameter.config());

      AdvancedCache<MagicKey, String> cache = this.<MagicKey, String>cache(0, cacheName).getAdvancedCache();
      AdvancedCache<MagicKey, String> owner = this.<MagicKey, String>cache(1, cacheName).getAdvancedCache();
      TransactionTable ownerTxTable = owner.getComponentRegistry().getTransactionTable();
      TransactionTable cacheTxTable = cache.getComponentRegistry().getTransactionTable();
      Method cleanupMethod = extractCleanupMethod();

      final MagicKey key = new MagicKey("key", owner);
      EmbeddedTransactionManager tm = (EmbeddedTransactionManager) cache.getTransactionManager();

      tm.begin();
      cache.put(key, "a");

      testParameter.beforeAdvanceTime(tm);

      //get the local gtx. should be the same as remote
      GlobalTransaction gtx = cacheTxTable.getGlobalTransaction(tm.getTransaction());
      assertTrue("RemoteTransaction must exists after key is locked!", ownerTxTable.containRemoteTx(gtx));

      //completedTxTimeout is 10'000 ms. we advance 11'000
      timeService.advance(COMPLETED_TX_TIMEOUT + 1000);

      //check if the remote-tx is eligible for timeout
      RemoteTransaction rtx = ownerTxTable.getRemoteTransaction(gtx);
      assertNotNull("RemoteTransaction must exists after key is locked!", rtx);
      assertTrue("RemoteTransaction is not eligible for timeout.", rtx.getCreationTime() - getCreationTimeCutoff() < 0);

      //instead of waiting for the reaper, invoke the method directly
      cleanupMethod.invoke(ownerTxTable);

      //it should keep the tx
      assertTrue("RemoteTransaction should be live after cleanup.", ownerTxTable.containRemoteTx(gtx));

      testParameter.afterAdvanceTime(tm);

      assertEquals("Wrong value in originator", "a", cache.get(key));
      assertEquals("Wrong value in owner", "a", owner.get(key));
   }

   public void testCheckTransactionRpcCommand() throws Exception {
      //default cache is transactional

      Cache<String, String> cache0 = cache(0);
      Cache<String, String> cache1 = cache(1);

      CommandsFactory factory = cache0.getAdvancedCache().getComponentRegistry().getCommandsFactory();

      RpcManager rpcManager = cache0.getAdvancedCache().getRpcManager();
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      ResponseCollector<Collection<GlobalTransaction>> collector = CheckTransactionRpcCommand.responseCollector();

      Address remoteAddress = cache1.getAdvancedCache().getRpcManager().getAddress();
      TransactionTable transactionTable = cache1.getAdvancedCache().getComponentRegistry().getTransactionTable();

      CheckTransactionRpcCommand rpcCommand = factory.buildCheckTransactionRpcCommand(Collections.emptyList());
      Collection<GlobalTransaction> result = rpcManager.invokeCommand(remoteAddress, rpcCommand, collector, rpcOptions)
            .toCompletableFuture()
            .join();

      assertTrue("Expected an empty collection but got: " + result, result.isEmpty());

      TransactionManager tm = cache1.getAdvancedCache().getTransactionManager();
      tm.begin();
      cache1.put("k", "v");

      rpcCommand = factory.buildCheckTransactionRpcCommand(transactionTable.getLocalGlobalTransaction());
      result = rpcManager.invokeCommand(remoteAddress, rpcCommand, collector, rpcOptions)
            .toCompletableFuture()
            .join();

      assertTrue("Expected an empty collection but got: " + result, result.isEmpty());

      tm.commit();

      GlobalTransaction nonExistingGtx = new GlobalTransaction(remoteAddress, false);
      nonExistingGtx.setId(-1);

      Collection<GlobalTransaction> list = Collections.singletonList(nonExistingGtx);

      rpcCommand = factory.buildCheckTransactionRpcCommand(list);
      result = rpcManager.invokeCommand(remoteAddress, rpcCommand, collector, rpcOptions)
            .toCompletableFuture()
            .join();
      assertEquals("Wrong list returned.", list, result);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createClusteredCaches(2, TestDataSCI.INSTANCE, builder);
      timeService = new ControlledTimeService();
      for (EmbeddedCacheManager cm : cacheManagers) {
         replaceComponent(cm, TimeService.class, timeService, true);
      }
   }

   private long getCreationTimeCutoff() {
      //copied from TransactionTable
      long beginning = timeService.time();
      return beginning - TimeUnit.MILLISECONDS.toNanos(COMPLETED_TX_TIMEOUT);
   }

   private enum TestLockMode implements LongTxTestParameter {
      PESSIMISTIC {
         @Override
         public String cacheName() {
            return "p_cache";
         }

         @Override
         public ConfigurationBuilder config() {
            ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.transaction()
                  .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
                  .lockingMode(LockingMode.PESSIMISTIC)
                  .completedTxTimeout(COMPLETED_TX_TIMEOUT);
            return builder;
         }

         @Override
         public void beforeAdvanceTime(EmbeddedTransactionManager tm) {
            //no-op
         }

         @Override
         public void afterAdvanceTime(EmbeddedTransactionManager tm) throws Exception {
            tm.commit();
         }
      },
      OPTIMISTIC {
         @Override
         public String cacheName() {
            return "o_cache";
         }

         @Override
         public ConfigurationBuilder config() {
            ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.transaction()
                  .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
                  .lockingMode(LockingMode.OPTIMISTIC)
                  .completedTxTimeout(COMPLETED_TX_TIMEOUT);
            return builder;
         }

         @Override
         public void beforeAdvanceTime(EmbeddedTransactionManager tm) {
            tm.getTransaction().runPrepare();
         }

         @Override
         public void afterAdvanceTime(EmbeddedTransactionManager tm) throws Exception {
            tm.getTransaction().runCommit(false);
            EmbeddedTransactionManager.dissociateTransaction();
         }
      }
   }

   private interface LongTxTestParameter {

      String cacheName();

      ConfigurationBuilder config();

      void beforeAdvanceTime(EmbeddedTransactionManager tm);

      void afterAdvanceTime(EmbeddedTransactionManager tm) throws Exception;

   }
}
