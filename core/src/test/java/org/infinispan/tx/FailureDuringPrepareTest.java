package org.infinispan.tx;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

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
      advancedCache(1).addInterceptor(new CommandInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            try {
               return super.visitPrepareCommand(ctx, command);
            } finally {
               //allow the prepare to succeed then crash
               throw new RuntimeException("Induced fault!");
            }
         }
      },2);

      tm(0).begin();

      cache(0).put("k","v");

      if (multipleResources) {
         tm(0).getTransaction().enlistResource(new XAResourceAdapter());
      }

      assertEquals(lockManager(0).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(1).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(2).getNumberOfLocksHeld(), 0);

      try {
         tm(0).commit();
         assert false;
      } catch (Exception e) {
         log.debug("Ignoring expected exception during prepare", e);
      }

      assertEquals(lockManager(0).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(1).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(2).getNumberOfLocksHeld(), 0);
   }


}
