package org.infinispan.tx.totalorder;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Tests if the locks are cleanup after a TimeoutException
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "tx.totalorder.CleanupAfterFailTest")
public class CleanupAfterFailTest extends MultipleCacheManagersTest {

   public void testTimeoutCleanup() throws Exception {
      final CountDownLatch block = new CountDownLatch(1);
      final CommandInterceptor interceptor = new BaseCustomInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            block.await();
            return invokeNextInterceptor(ctx, command);
         }
      };
      final InterceptorChain chain = TestingUtil.extractComponent(cache(1), InterceptorChain.class);
      final Object key = new MagicKey(cache(1));
      try {
         chain.addInterceptor(interceptor, 0);
         tm(0).begin();
         cache(0).put(key, "v");
         tm(0).commit();
         fail("Rollback expected!");
      } catch (RollbackException e) {
         //expected
      } finally {
         block.countDown();
         chain.removeInterceptor(0);
      }

      assertNoTransactions();
      assertNoLocks();
   }

   public void testTimeoutCleanupInLocalNode() throws Exception {
      final CountDownLatch block = new CountDownLatch(1);
      final CommandInterceptor interceptor = new BaseCustomInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            if (!ctx.isOriginLocal()) {
               block.await();
            }
            return invokeNextInterceptor(ctx, command);
         }
      };
      final InterceptorChain chain = TestingUtil.extractComponent(cache(0), InterceptorChain.class);
      final Object key1 = new MagicKey(cache(0));
      final Object key2 = new MagicKey(cache(1));
      try {
         chain.addInterceptor(interceptor, 0);
         tm(0).begin();
         cache(0).put(key1, "v1");
         cache(0).put(key2, "v2");
         tm(0).commit();
         fail("Rollback expected!");
      } catch (RollbackException e) {
         //expected
      } finally {
         block.countDown();
         chain.removeInterceptor(0);
      }

      cache(0).put(key1, "v3");
      cache(0).put(key2, "v4");

      assertCacheValue(key1, "v3");
      assertCacheValue(key2, "v4");

      assertNoTransactions();
      assertNoLocks();
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction()
            .transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .syncCommitPhase(true)
            .syncRollbackPhase(true)
            .useSynchronization(false)
            .recovery().disable();
      dcc.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true);
      dcc.clustering().hash()
            .numOwners(1)
            .numSegments(60);
      dcc.versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   private void assertCacheValue(Object key, Object value) {
      for (Cache cache : caches()) {
         assertEquals(cache.get(key), value, "Wrong value for cache " + address(cache) + ". key=" + key);
      }
   }

   private void assertNoLocks() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache cache : caches()) {
               if (TestingUtil.extractComponent(cache, TotalOrderManager.class).hasAnyLockAcquired()) {
                  return false;
               }
            }
            return true;
         }
      });
   }

}
