package org.infinispan.tx.exception;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tx.dld.BaseDldTest;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(testName = "tx.exception.ReplicationExceptionTest")
public class ReplicationExceptionTest extends MultipleCacheManagersTest {
   private ControlledRpcManager crm0;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      registerCacheManager(TestCacheManagerFactory.createCacheManager(config, true));
      registerCacheManager(TestCacheManagerFactory.createCacheManager(config, true));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      crm0 = BaseDldTest.replaceRpcManager(cache(0));
   }


   public void testReplicationFailure() throws Exception {
      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
      tm.begin();
      cache(0).put("k0","v");
      try {
         tm.commit();
         assert false;
      } catch (RollbackException e) {
         //expected
      }
   }
}
