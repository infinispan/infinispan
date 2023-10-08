package org.infinispan.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.transaction.RollbackException;
import jakarta.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Test for ISPN-8533
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "tx.PessimisticDeadlockTest")
public class PessimisticDeadlockTest extends MultipleCacheManagersTest {

   public void testDeadlock(Method method) throws Exception {
      assertEquals(4, managers().length);
      final String key = method.getName();
      assertOwnership(key);
      dropLockCommandInPrimary();

      //both transactions will send the LockControlCommand to cache1 and cache2.
      //cache1 will discard the command (it will be killed)
      //cache2 will acquire the backup lock.
      Future<Boolean> tx1 = runTransaction(key, "tx1");
      Future<Boolean> tx2 = runTransaction(key, "tx2");

      //make sure the backup is acquired in cache2
      awaitBackupLocks(key);

      killMember(1);
      assertEquals(3, managers().length);
      //check if the cache2 is actually the primary owner.
      //both transactions should re-send the LockControlCommand
      assertOwnership(key);

      //both txs must complete successfully but we don't know which one was last
      //the test fails here if we have a deadlock!
      assertTrue(tx1.get(30, TimeUnit.SECONDS));
      assertTrue(tx2.get(30, TimeUnit.SECONDS));

      //just make sure everything is ok
      String result = this.<String, String>cache(0).get(key);
      for (Cache<String, String> cache : this.<String, String>caches()) {
         assertEquals(result, cache.get(key));
      }
      assertNoTransactions();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      builder.clustering().hash().consistentHashFactory(new ControlledConsistentHashFactory.Default(1, 2))
            .numSegments(1)
            .numOwners(2);
      createClusteredCaches(4, ControlledConsistentHashFactory.SCI.INSTANCE, builder);
   }

   private Future<Boolean> runTransaction(String key, String value) {
      return fork(() -> {
         AdvancedCache<String, String> cache = this.<String, String>cache(0).getAdvancedCache();
         TransactionManager tm = cache.getTransactionManager();
         tm.begin();
         cache.put(key, value);
         try {
            tm.commit();
            return true;
         } catch (RollbackException e) {
            return false;
         }
      });
   }

   private void dropLockCommandInPrimary() {
      TestingUtil.wrapInboundInvocationHandler(cache(1), DropLockCommandHandler::new);
   }

   private void awaitBackupLocks(String key) {
      eventually(() -> {
         TransactionTable table = TestingUtil.getTransactionTable(cache(2));
         Collection<RemoteTransaction> remoteTransactions = table.getRemoteTransactions();
         if (remoteTransactions.size() != 2) {
            return false;
         }
         for (RemoteTransaction rtx : remoteTransactions) {
            if (!rtx.getBackupLockedKeys().contains(key)) {
               return false;
            }
         }
         return true;
      });
   }


   private void assertOwnership(String key) {
      for (Cache<?, ?> cache : caches()) {
         Collection<Address> writeOwners = cache.getAdvancedCache().getDistributionManager().getCacheTopology()
               .getDistribution(key).writeOwners();
         assertEquals(Arrays.asList(address(1), address(2)), writeOwners);
      }
   }

   private static class DropLockCommandHandler extends AbstractDelegatingHandler {
      DropLockCommandHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (!(command instanceof LockControlCommand)) {
            delegate.handle(command, reply, order);
         }
      }
   }

}
