package org.infinispan.persistence;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test if keys are properly passivated and reloaded in local mode (to ensure fix for ISPN-2712 did no break local mode).
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "persistence.LocalModePassivationTest")
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

   protected void configureConfiguration(ConfigurationBuilder cb) {

   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheStoreDir = new File(TestingUtil.tmpDirectory(this.getClass()));
      TestingUtil.recursiveFileRemove(cacheStoreDir);

      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .eviction().maxEntries(1000).strategy(EvictionStrategy.LIRS)
            .locking().lockAcquisitionTimeout(20000)
            .concurrencyLevel(5000)
            .useLockStriping(false).writeSkewCheck(false).isolationLevel(IsolationLevel.READ_COMMITTED)
            .dataContainer().storeAsBinary()
            .persistence().passivation(passivationEnabled).addSingleFileStore().location(cacheStoreDir.getAbsolutePath())
            .fetchPersistentState(true)
            .ignoreModifications(false)
            .preload(false)
            .purgeOnStartup(false);

      configureConfiguration(builder);

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

      AdvancedLoadWriteStore store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache());
      int keysInCacheStore = PersistenceUtil.count(store, null);

      if (passivationEnabled) {
         assertEquals(numKeys, keysInDataContainer + keysInCacheStore);
      } else {
         assertEquals(numKeys, keysInCacheStore);
      }

      // check if keys survive restart
      cache().stop();
      cache().start();

      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache());
      assertEquals(numKeys, PersistenceUtil.count(store, null));

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache().get(i));
      }
   }

   public void testSizeWithEvictedEntries() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }
      assertFalse("Data Container should not have all keys", numKeys == cache.getAdvancedCache().getDataContainer().size());
      assertEquals(numKeys, cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).size());
   }

   public void testSizeWithEvictedEntriesAndFlags() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }
      assertFalse("Data Container should not have all keys", numKeys == cache.getAdvancedCache().getDataContainer().size());
      assertEquals(cache.getAdvancedCache().getDataContainer().size(), cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).size());
      // Skip cache store only prevents writes not reads
      assertEquals(300, cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_STORE).size());
   }

   public void testKeySetWithEvictedEntries() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      assertFalse("Data Container should not have all keys", numKeys == cache.getAdvancedCache().getDataContainer().size());
      Set<Object> keySet = cache.keySet();
      for (int i = 0; i < numKeys; i++) {
         assertTrue("Key: " + i + " was not found!", keySet.contains(i));
      }
   }

   public void testKeySetWithEvictedEntriesAndFlags() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      AdvancedCache<Object, Object> flagCache = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      DataContainer dc = flagCache.getDataContainer();
      assertFalse("Data Container should not have all keys", numKeys == dc.size());
      Set<Object> keySet = flagCache.keySet();
      assertEquals(dc.size(), keySet.size());
      for (Object key : dc.keySet()) {
         assertTrue("Key: " + key + " was not found!", keySet.contains(key));
      }
   }

   public void testEntrySetWithEvictedEntries() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      assertFalse("Data Container should not have all keys", numKeys == cache.getAdvancedCache().getDataContainer().size());
      Set<Map.Entry<Object, Object>> entrySet = cache.entrySet();
      assertEquals(numKeys, entrySet.size());

      Map<Object, Object> map = new HashMap<Object, Object>(entrySet.size());
      for (Map.Entry<Object, Object> entry : entrySet) {
         map.put(entry.getKey(), entry.getValue());
      }

      for (int i = 0; i < numKeys; i++) {
         assertEquals("Key/Value mismatch!", i, map.get(i));
      }
   }

   public void testEntrySetWithEvictedEntriesAndFlags() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      AdvancedCache<Object, Object> flagCache = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      DataContainer dc = flagCache.getDataContainer();
      assertFalse("Data Container should not have all keys", numKeys == dc.size());
      Set<Map.Entry<Object, Object>> entrySet = flagCache.entrySet();
      assertEquals(dc.size(), entrySet.size());

      Set<InternalCacheEntry> entries = dc.entrySet();
      Map<Object, Object> map = new HashMap<Object, Object>(entrySet.size());
      for (Map.Entry<Object, Object> entry : entrySet) {
         map.put(entry.getKey(), entry.getValue());
      }

      for (InternalCacheEntry entry : entries) {
         assertEquals("Key/Value mismatch!", entry.getValue(), map.get(entry.getKey()));
      }
   }

   public void testValuesWithEvictedEntries() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      assertFalse("Data Container should not have all keys", numKeys == cache.getAdvancedCache().getDataContainer().size());
      Collection<Object> values = cache.values();
      for (int i = 0; i < numKeys; i++) {
         assertTrue("Value: " + i + " was not found!", values.contains(i));
      }
   }

   public void testValuesWithEvictedEntriesAndFlags() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache.put(i, i);
      }

      AdvancedCache<Object, Object> flagCache = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      DataContainer dc = flagCache.getDataContainer();
      assertFalse("Data Container should not have all keys", numKeys == dc.size());
      Collection<Object> values = flagCache.values();
      assertEquals(dc.size(), values.size());

      Collection<Object> dcValues = dc.values();
      for (Object dcValue : dcValues) {
         assertTrue("Value: " + dcValue + " was not found!", values.contains(dcValue));
      }
   }

   public void testStoreAndLoadWithGetEntry() {
      final int numKeys = 300;
      for (int i = 0; i < numKeys; i++) {
         cache().put(i, i);
      }

      int keysInDataContainer = cache().getAdvancedCache().getDataContainer().keySet().size();

      assertTrue(keysInDataContainer != numKeys); // some keys got evicted

      AdvancedLoadWriteStore store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache());
      int keysInCacheStore = PersistenceUtil.count(store, null);

      if (passivationEnabled) {
         assertEquals(numKeys, keysInDataContainer + keysInCacheStore);
      } else {
         assertEquals(numKeys, keysInCacheStore);
      }

      // check if keys survive restart
      cache().stop();
      cache().start();

      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache());
      assertEquals(numKeys, PersistenceUtil.count(store, null));

      for (int i = 0; i < numKeys; i++) {
         assertEquals(i, cache.getAdvancedCache().getCacheEntry(i).getValue());
      }
   }
}
