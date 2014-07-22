package org.infinispan.tx;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * Test for https://issues.jboss.org/browse/ISPN-1093.
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "tx.FailureWith1PCTest")
public class FailureWith1PCTest extends MultipleCacheManagersTest {

   boolean fail = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.clustering().hash().numOwners(3);
      createCluster(c, 3);
      waitForClusterToForm();
   }

   public void testInducedFailureOn1pc() throws Exception {

      cache(1).getAdvancedCache().addInterceptor(new CommandInterceptor() {

         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            if (fail)
               throw new RuntimeException("Induced exception");
            else
               return invokeNextInterceptor(ctx, command);
         }
      }, 1);

      tm(0).begin();
      cache(0).put("k", "v");

      try {
         tm(0).commit();
         assert false : "Exception expected";
      } catch (Exception e) {
         log.debug("Ignoring expected exception during 1-phase prepare", e);
      }

      fail = false;

      assertExpectedState(0);
      assertExpectedState(1);
      assertExpectedState(2);

   }

   private void assertExpectedState(int index) {
      assertNull(cache(index).get("k"));
      assert !lockManager(index).isLocked("k");
      assert TestingUtil.getTransactionTable(cache(index)).getLocalTxCount() == 0;
      assert TestingUtil.getTransactionTable(cache(index)).getRemoteTxCount() == 0;
   }
}
