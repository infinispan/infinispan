package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.remoting.inboundhandler.BlockHandler;
import org.infinispan.remoting.inboundhandler.ControllingPerCacheInboundInvocationHandler;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

@Test(testName = "tx.RollbackBeforePrepareTest", groups = "functional")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC})
public class RollbackBeforePrepareTest extends MultipleCacheManagersTest {

   public static final long REPL_TIMEOUT = 1000;
   public static final long LOCK_TIMEOUT = 500;
   protected int numOwners = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(cacheMode, true);
      config
            .locking().lockAcquisitionTimeout(LOCK_TIMEOUT)
            .clustering().remoteTimeout(REPL_TIMEOUT)
            .clustering().hash().numOwners(numOwners)
            .transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .transaction().completedTxTimeout(3600000);

      createCluster(config, 3);
      waitForClusterToForm();
      FailPrepareInterceptor failPrepareInterceptor = new FailPrepareInterceptor();
      extractInterceptorChain(advancedCache(2)).addInterceptor(failPrepareInterceptor, 1);
   }


   public void testCommitNotSentBeforeAllPrepareAreAck() throws Exception {
      ControllingPerCacheInboundInvocationHandler blockingHandler = ControllingPerCacheInboundInvocationHandler.replace(cache(1));
      BlockHandler prepare = blockingHandler.blockRpcBefore(PrepareCommand.class);
      BlockHandler rollback = blockingHandler.blockRpcBefore(RollbackCommand.class);

      try {
         cache(0).put("k", "v");
         fail();
      } catch (Exception e) {
         //expected
      }

      prepare.awaitUntilBlocked(Duration.of(15, ChronoUnit.SECONDS));
      rollback.awaitUntilBlocked(Duration.of(15, ChronoUnit.SECONDS));

      rollback.unblock();
      rollback.awaitUntilCommandCompleted(Duration.of(15, ChronoUnit.SECONDS));

      prepare.unblock();

      eventually(() -> {
         int remoteTxCount0 = TestingUtil.getTransactionTable(cache(0)).getRemoteTxCount();
         int remoteTxCount1 = TestingUtil.getTransactionTable(cache(1)).getRemoteTxCount();
         int remoteTxCount2 = TestingUtil.getTransactionTable(cache(2)).getRemoteTxCount();
         log.tracef("remote0=%s, remote1=%s, remote2=%s", remoteTxCount0, remoteTxCount1, remoteTxCount2);
         return remoteTxCount0 == 0 && remoteTxCount1 == 0 && remoteTxCount2 == 0;
      });

      assertNull(cache(0).get("k"));
      assertNull(cache(1).get("k"));
      assertNull(cache(2).get("k"));

      assertNotLocked("k");
   }

   public static class FailPrepareInterceptor extends DDAsyncInterceptor {

      CountDownLatch failureFinish = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            throw new TimeoutException("Induced!");
         } finally {
            failureFinish.countDown();
         }
      }
   }
}
