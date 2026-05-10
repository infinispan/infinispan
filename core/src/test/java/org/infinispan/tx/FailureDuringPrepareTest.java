package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.FailureDuringPrepareTest")
@CleanupAfterMethod
public class FailureDuringPrepareTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.clustering().hash().numOwners(3);
      createCluster(c, 3);
      waitForClusterToForm();
   }

   public void testResourceCleanedIfPrepareFails() throws Exception {
      runTest(false);
   }

   public void testResourceCleanedIfPrepareFails2() throws Exception {
      runTest(true);
   }

   private void runTest(boolean multipleResources) throws NotSupportedException, SystemException, RollbackException {
      extractInterceptorChain(advancedCache(1)).addInterceptor(new FailInterceptor(), 2);

      tm(0).begin();

      cache(0).put("k","v");

      if (multipleResources) {
         tm(0).getTransaction().enlistResource(new XAResourceAdapter());
      }

      assertEquals(0, lockManager(0).getNumberOfLocksHeld());
      assertEquals(0, lockManager(1).getNumberOfLocksHeld());
      assertEquals(0, lockManager(2).getNumberOfLocksHeld());

      try {
         tm(0).commit();
         fail();
      } catch (Exception e) {
         log.debug("Ignoring expected exception during prepare", e);
      }

      assertEquals(0, lockManager(0).getNumberOfLocksHeld());
      assertEquals(0, lockManager(1).getNumberOfLocksHeld());
      assertEquals(0, lockManager(2).getNumberOfLocksHeld());
   }


   static class FailInterceptor extends DDAsyncInterceptor {
      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            //allow the prepare to succeed then crash
            throw new RuntimeException("Induced fault!");
         });
      }
   }
}
