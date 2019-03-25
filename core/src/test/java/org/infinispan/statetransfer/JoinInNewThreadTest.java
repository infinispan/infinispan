package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.Future;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.Test;

/**
 * Test that a node started in a different thread can join the cluster.
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Test(testName = "statetransfer.JoinInNewThreadTest", groups = "functional")
@CleanupAfterMethod
public class JoinInNewThreadTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      // Do nothing here
   }

   public void testJoinInNewThread() throws Exception {
      TestResourceTracker.setThreadTestName(JoinInNewThreadTest.class.getName());

      ConfigurationBuilder replCfg = new ConfigurationBuilder();
      replCfg.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, SECONDS);

      // Connect 2 channels
      addClusterEnabledCacheManager(replCfg);
      addClusterEnabledCacheManager(replCfg);
      waitForClusterToForm();

      Future<Void> future = fork(() -> {
         TestResourceTracker.testThreadStarted(this);
         addClusterEnabledCacheManager(replCfg);
         waitForClusterToForm();
      });
      future.get(30, SECONDS);
   }
}
