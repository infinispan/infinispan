package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Short test to reproduce the scenario from ISPN-2712/MODE-1754 (https://issues.jboss.org/browse/MODE-2712, https://issues.jboss.org/browse/MODE-1754).
 * <p/>
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
   private ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() {
      tmpDir = new File(TestingUtil.tmpDirectory(this.getClass()));
      TestingUtil.recursiveFileRemove(tmpDir);

      // reproduce the MODE-1754 config as closely as possible
      builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .eviction().maxEntries(1000).strategy(EvictionStrategy.LIRS)
            .locking().lockAcquisitionTimeout(20000)
            .concurrencyLevel(5000) // lowering this to 50 makes the test pass also on 5.2 but it's just a temporary workaround
            .useLockStriping(false).writeSkewCheck(false).isolationLevel(IsolationLevel.READ_COMMITTED)
            .dataContainer().storeAsBinary()
            .clustering().sync().replTimeout(20000)
            .stateTransfer().timeout(240000).fetchInMemoryState(false).chunkSize(Integer.MAX_VALUE)
            .persistence().passivation(false).addSingleFileStore().location(new File(tmpDir, "store0").getAbsolutePath()).shared(false).preload(false)
            .fetchPersistentState(true)
            .ignoreModifications(false)
            .purgeOnStartup(false);

      createCluster(builder, 1);
      waitForClusterToForm();
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDir);
   }

   public void testStateTransfer() throws Exception {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache(0).put(i, i);
      }
      log.info("Finished putting keys");

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache(0).get(i));
      }

      log.info("Adding a new node ..");
      builder.persistence().clearStores().addSingleFileStore().location(new File(tmpDir, "store1").getAbsolutePath())  // make sure this node writes in a different location
            .fetchPersistentState(true)
            .ignoreModifications(false)
            .purgeOnStartup(false);

      addClusterEnabledCacheManager(builder);
      log.info("Added a new node");

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache(1).get(i));   // some keys are lost in 5.2
      }
   }
}
