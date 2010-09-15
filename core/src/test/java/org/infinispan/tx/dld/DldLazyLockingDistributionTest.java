package org.infinispan.tx.dld;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.Executor;

/**
 * Tests deadlock detection when t1 acquire (k1, k2) and te acquires (k2, k1).
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.dld.DldLazyLockingDistributionTest")
public class DldLazyLockingDistributionTest extends BaseDldLazyLockingTest {

   private KeyAffinityService cas;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setUnsafeUnreliableReturnValues(true);
      config.setNumOwners(1);
      config.setEnableDeadlockDetection(true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(config, true);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(config, true);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1));

      cas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0), new Executor() {
         public void execute(Runnable command) {
            new Thread(command).start();
         }
      }, new RndKeyGenerator(), 2, true);

      rpcManager0 = DldLazyLockingReplicationTest.replaceRpcManager(cache(0));
      rpcManager1 = DldLazyLockingReplicationTest.replaceRpcManager(cache(1));
   }

   public void testSymmetricDeadlock() {
      Object k0 = cas.getKeyForAddress(address(0));
      Object k1 = cas.getKeyForAddress(address(1));
      testSymmetricDeadlock(k0, k1);
   }

   @AfterClass
   public void destroyKeyService() {
      cas.stop();
   }

}
