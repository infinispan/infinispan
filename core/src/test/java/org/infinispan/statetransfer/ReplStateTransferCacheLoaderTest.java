package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Short test to reproduce the scenario from ISPN-2712/MODE-1754 (https://issues.jboss.org/browse/MODE-2712, https://issues.jboss.org/browse/MODE-1754).
  * This test passes on 5.1.x but fails on 5.2.0 without the fix.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplStateTransferCacheLoaderTest")
@CleanupAfterMethod
public class ReplStateTransferCacheLoaderTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(ReplStateTransferCacheLoaderTest.class);

   private File tmpDir;
   private GlobalConfigurationBuilder globalBuilder;
   private ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() {
      tmpDir = new File(CommonsTestingUtil.tmpDirectory(this.getClass()));
      Util.recursiveFileRemove(tmpDir);

      globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.globalState().persistentLocation(tmpDir.getPath());

      // reproduce the MODE-1754 config as closely as possible
      builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .memory().maxCount(1000)
            .locking().lockAcquisitionTimeout(20000)
            .concurrencyLevel(5000) // lowering this to 50 makes the test pass also on 5.2 but it's just a temporary workaround
            .useLockStriping(false).isolationLevel(IsolationLevel.READ_COMMITTED)
            .clustering().remoteTimeout(20000)
            .stateTransfer().timeout(240000).fetchInMemoryState(false).chunkSize(10000)
            .persistence().addSingleFileStore().location(new File(tmpDir, "store0").getAbsolutePath());

      createCluster(globalBuilder, builder, 1);
      waitForClusterToForm();
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDir);
   }

   public void testStateTransfer() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache(0).put(i, i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache(0).get(i));
      }

      log.info("Adding a new node ..");
      // make sure this node writes in a different location
      builder.persistence().clearStores().addSingleFileStore().location(new File(tmpDir, "store0").getAbsolutePath()).fetchPersistentState(true);

      addClusterEnabledCacheManager(globalBuilder, builder);
      log.info("Added a new node");

      for (int i = 0; i < numKeys; i++) {
         // some keys are lost in 5.2
         assertEquals(i, cache(1).get(i));
      }
   }
}
