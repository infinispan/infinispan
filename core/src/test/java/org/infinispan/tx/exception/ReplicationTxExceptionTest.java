package org.infinispan.tx.exception;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ReplicationTxExceptionTest")
public class ReplicationTxExceptionTest extends MultipleCacheManagersTest {
   private ControlledRpcManager controlledRpcManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      Cache<?, ?> cache = cache(0);
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      controlledRpcManager = new ControlledRpcManager(rpcManager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
   }

   public void testReplicationFailure() throws Exception {
      controlledRpcManager.failFor(PrepareCommand.class);
      try {
         TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
         tm.begin();
         cache(0).put("k0", "v");
         try {
            tm.commit();
            assert false;
         } catch (RollbackException e) {
            //expected
         }
      } finally {
         controlledRpcManager.stopFailing();
      }
   }
}
