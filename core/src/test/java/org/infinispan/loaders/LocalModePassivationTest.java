package org.infinispan.loaders;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test if keys are properly passivated and reloaded in local mode (to ensure fix for ISPN-2712 did no break local mode).
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "loaders.LocalModePassivationTest")
@CleanupAfterMethod
public class LocalModePassivationTest extends SingleCacheManagerTest {

   private File cacheStoreDir;

   private final boolean passivationEnabled;

   protected LocalModePassivationTest() {
      passivationEnabled = true;
   }

   protected LocalModePassivationTest(boolean passivationEnabled) {
      this.passivationEnabled = passivationEnabled;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheStoreDir = new File(TestingUtil.tmpDirectory(this));
      TestingUtil.recursiveFileRemove(cacheStoreDir);

      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .eviction().maxEntries(1000).strategy(EvictionStrategy.LIRS)
            .locking().lockAcquisitionTimeout(20000)
            .concurrencyLevel(5000)
            .useLockStriping(false).writeSkewCheck(false).isolationLevel(IsolationLevel.READ_COMMITTED)
            .dataContainer().storeAsBinary()
            .loaders().passivation(passivationEnabled).preload(false).addFileCacheStore().location(cacheStoreDir.getAbsolutePath())
            .fetchPersistentState(true)
            .purgerThreads(3)
            .purgeSynchronously(true)
            .ignoreModifications(false)
            .purgeOnStartup(false);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(cacheStoreDir);
   }

   public void testStoreAndLoad() throws Exception {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache().put(i, i);
      }

      int keysInDataContainer = cache().getAdvancedCache().getDataContainer().keySet().size();

      assertTrue(keysInDataContainer != numKeys); // some keys got evicted

      CacheLoaderManager cml = cache().getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      int keysInCacheStore = cml.getCacheLoader().loadAll().size();

      if (passivationEnabled) {
         assertEquals(numKeys, keysInDataContainer + keysInCacheStore);
      } else {
         assertEquals(numKeys, keysInCacheStore);
      }

      // check if keys survive restart
      cache().stop();
      cache().start();

      cml = cache().getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      assertEquals(numKeys, cml.getCacheLoader().loadAll().size());

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache().get(i));
      }
   }
}
