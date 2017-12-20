package org.infinispan.tx.exception;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ReplicationTxExceptionTest")
public class ReplicationTxExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
   }

   public void testReplicationFailure() throws Exception {
      Cache<?, ?> cache = cache(0);
      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache);
      Future<Void> future = fork(() -> controlledRpcManager.expectCommand(VersionedPrepareCommand.class).fail());
      try {
         TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
         tm.begin();
         cache(0).put("k0", "v");
         Exceptions.expectException(RollbackException.class, tm::commit);
      } finally {
         controlledRpcManager.revertRpcManager(cache);
         future.get(30, TimeUnit.SECONDS);
      }
   }
}
