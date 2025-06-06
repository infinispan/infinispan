package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.util.concurrent.CountDownLatch;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.TimeoutException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
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
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      Mocks.blockInboundCacheRpcCommand(cache(1), checkPoint, c -> c.getClass().equals(PrepareCommand.class));

      try {
         cache(0).put("k", "v");
         fail();
      } catch (Exception e) {
         //expected
      }

      //this will also cause a replication timeout
      allowRollbackToRun();

      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      //give some time for the prepare to execute
      Thread.sleep(3000);

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

   /**
    * by using timeouts here the worse case is to have false positives, i.e. the test to pass when it shouldn't. no
    * false negatives should be possible. In single threaded suit runs this test will generally fail in order
    * to highlight a bug.
    */
   private static void allowRollbackToRun() throws InterruptedException {
      Thread.sleep(REPL_TIMEOUT * 15);
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
