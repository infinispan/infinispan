package org.infinispan.globalstate;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "globalstate.ThreeNodeDistGlobalStateRestartTest", groups = "functional")
public class ThreeNodeDistGlobalStateRestartTest extends AbstractGlobalStateRestartTest {

   @Override
   protected int getClusterSize() {
      return 3;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
   }

   public void testGracefulShutdownAndRestart() throws Throwable {
      shutdownAndRestart(-1, false);
   }

   public void testGracefulShutdownAndRestartReverseOrder() throws Throwable {
      shutdownAndRestart(-1, true);
   }

   public void testFailedRestartWithExtraneousCoordinator() throws Throwable {
      shutdownAndRestart(0, false);
   }

   public void testFailedRestartWithExtraneousNode() throws Throwable {
      shutdownAndRestart(1, false);
   }

   public void testPersistentStateIsDeletedAfterRestart() throws Throwable {
      shutdownAndRestart(-1, false);

      restartWithoutGracefulShutdown();
   }

   public void testClusterRecoveryAfterRestart() throws Throwable {
      shutdownAndRestart(-1, false);

      killMember(0, CACHE_NAME, false);

      assertEquals(DATA_SIZE, (long) cache(0, CACHE_NAME).size());
      assertEquals(DATA_SIZE, (long) cache(1, CACHE_NAME).size());
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));
   }
}
