package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.Ownership;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests if the conditional commands correctly fetch the value from cache loader even with the skip cache load/store
 * flags.
 * <p/>
 * The configuration used is a non-tx distributed cache without passivation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "persistence.ClusteredConditionalCommandTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class ClusteredConditionalCommandTest extends MultipleCacheManagersTest {
   private static final String PRIVATE_STORE_CACHE_NAME = "private-store-cache";
   private static final String SHARED_STORE_CACHE_NAME = "shared-store-cache";
   private final String key = getClass().getSimpleName() + "-key";
   private final String value1 = getClass().getSimpleName() + "-value1";
   private final String value2 = getClass().getSimpleName() + "-value2";
   private final boolean passivation;

   public ClusteredConditionalCommandTest() {
      this(false, false);
   }

   protected ClusteredConditionalCommandTest(boolean transactional, boolean passivation) {
      this.transactional = transactional;
      this.passivation = passivation;
      this.cacheMode = CacheMode.DIST_SYNC;
   }

   private ConfigurationBuilder createConfiguration(String storeName, boolean shared, boolean transactional,
                                                           boolean passivation, int storePrefix) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, transactional);
      builder.statistics().enable();
      builder.persistence()
            .passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + (shared ? "-shared" : storePrefix))
            .fetchPersistentState(false)
            .shared(shared);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return builder;
   }

   @AfterMethod
   public void afterMethod() {
      if (passivation) {
         for (EmbeddedCacheManager cacheManager : cacheManagers) {
            ActivationManager activationManager = TestingUtil.extractComponent(cacheManager.getCache(PRIVATE_STORE_CACHE_NAME),
                  ActivationManager.class);
            // Make sure no activations are pending, which could leak between tests
            eventuallyEquals((long) 0, activationManager::getPendingActivationCount);
         }
      }
   }

   private static <K, V> void writeToStore(CacheHelper<K, V> cacheHelper, Ownership ownership, K key, V value) {
      MarshallableEntry entry = MarshalledEntryUtil.create(key, value, cacheHelper.cache(ownership));
      cacheHelper.cacheStore(ownership).write(entry);
   }

   private <K, V> CacheHelper<K, V> create(List<Cache<K, V>> cacheList) {
      CacheHelper<K, V> cacheHelper = new CacheHelper<>();
      for (Cache<K, V> cache : cacheList) {
         ClusteringDependentLogic clusteringDependentLogic = extractComponent(cache, ClusteringDependentLogic.class);
         DistributionInfo distributionInfo = clusteringDependentLogic.getCacheTopology().getDistribution(key);
         log.debugf("owners for key %s are %s", key, distributionInfo.writeOwners());
         if (distributionInfo.isPrimary()) {
            log.debug("Cache " + address(cache) + " is the primary owner");
            assertTrue(cacheHelper.addCache(Ownership.PRIMARY, cache));
         } else if (distributionInfo.isWriteBackup()) {
            log.debug("Cache " + address(cache) + " is the backup owner");
            assertTrue(cacheHelper.addCache(Ownership.BACKUP, cache));
         } else {
            log.debug("Cache " + address(cache) + " is the non owner");
            assertTrue(cacheHelper.addCache(Ownership.NON_OWNER, cache));
         }
      }
      return cacheHelper;
   }

   private void doTest(String cacheName, ConditionalOperation operation, Ownership ownership,
                       Flag flag, boolean shared) {
      if (shared && passivation) {
         // Skip the test, shared stores don't support passivation
         return;
      }

      List<Cache<String, String>> cacheList = getCaches(cacheName);
      // These are not valid test combinations - so just ignore them
      if (shared && passivation) {
         throw new SkipException("Shared passivation is not supported");
      }
      waitForNoRebalance(cacheList);
      final CacheHelper<String, String> cacheHelper = create(cacheList);
      final boolean skipLoad = flag == Flag.SKIP_CACHE_LOAD || flag == Flag.SKIP_CACHE_STORE;
      assertEmpty(cacheList);
      initStore(cacheHelper, shared);

      try {
         if (flag != null) {
            operation.execute(cacheHelper.cache(ownership).getAdvancedCache().withFlags(flag), key, value1, value2);
         } else {
            operation.execute(cacheHelper.cache(ownership), key, value1, value2);
         }
      } catch (Exception e) {
         //some operation are allowed to fail. e.g. putIfAbsent.
         //we only check the final value
         log.debug(e);
      }

      assertLoadAfterOperation(cacheHelper, operation, ownership, skipLoad);

      Cache<String, String> primary = cacheHelper.cache(Ownership.PRIMARY);
      Cache<String, String> backup = cacheHelper.cache(Ownership.BACKUP);
      assertEquals(operation.finalValue(value1, value2, skipLoad), primary.get(key));
      if (backup != null) {
         assertEquals(operation.finalValue(value1, value2, skipLoad), backup.get(key));
      }
      //don't test in non_owner because it generates remote gets and they can cross tests causing random failures.
   }

   protected <K, V> void assertLoadAfterOperation(CacheHelper<K, V> cacheHelper, ConditionalOperation operation, Ownership ownership, boolean skipLoad) {
      //in non-transactional caches, only the primary owner will load from store
      // backup will load only in case that it is origin (and needs previous value)
      assertLoad(cacheHelper, skipLoad ? 0 : 1, 0, 0);
   }

   protected final <K, V> void assertLoad(CacheHelper<K, V> cacheHelper, int primaryOwner, int backupOwner, int nonOwner) {
      assertEquals("primary owner load", primaryOwner, cacheHelper.loads(Ownership.PRIMARY));
      assertEquals("backup owner load", backupOwner, cacheHelper.loads(Ownership.BACKUP));
      assertEquals("non owner load", nonOwner, cacheHelper.loads(Ownership.NON_OWNER));
   }

   private <K, V> void assertEmpty(List<Cache<K, V>> cacheList) {
      for (Cache<K, V> cache : cacheList) {
         assertTrue(cache + ".isEmpty()", cache.isEmpty());
      }
   }

   private void initStore(CacheHelper<String, String> cacheHelper, boolean shared) {
      DummyInMemoryStore primaryStore = cacheHelper.cacheStore(Ownership.PRIMARY);
      DummyInMemoryStore backupStore = cacheHelper.cacheStore(Ownership.BACKUP);
      DummyInMemoryStore nonOwnerStore = cacheHelper.cacheStore(Ownership.NON_OWNER);
      if (shared) {
         writeToStore(cacheHelper, Ownership.PRIMARY, key, value1);
         assertTrue(primaryStore.contains(key));
         if (backupStore != null) {
            assertTrue(backupStore.contains(key));
         }
         assertTrue(nonOwnerStore.contains(key));
      } else {
         writeToStore(cacheHelper, Ownership.PRIMARY, key, value1);
         assertTrue(primaryStore.contains(key));
         if (backupStore != null) {
            writeToStore(cacheHelper, Ownership.BACKUP, key, value1);
            assertTrue(backupStore.contains(key));
         }
         assertFalse(nonOwnerStore.contains(key));
      }

      cacheHelper.resetStats(Ownership.PRIMARY);
      cacheHelper.resetStats(Ownership.BACKUP);
      cacheHelper.resetStats(Ownership.NON_OWNER);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster( 3);

      int index = 0;
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         if (!passivation) {
            cacheManager.defineConfiguration(SHARED_STORE_CACHE_NAME,
                  createConfiguration(getClass().getSimpleName(), true, transactional,
                        false, index).build());
         }
         cacheManager.defineConfiguration(PRIVATE_STORE_CACHE_NAME,
                                          createConfiguration(getClass().getSimpleName(), false, transactional,
                                                              passivation, index).build());
         index++;
      }
   }

   public void testPutIfAbsentOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD,
             true);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD,
             false);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testPutIfAbsentOnPrimaryOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, null, true);
   }

   public void testPutIfAbsentOnPrimaryOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD,
             false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, null, false);
   }

   public void testPutIfAbsentOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD,
             true);
   }

   public void testPutIfAbsentOnNonOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD,
             false);
   }

   public void testPutIfAbsentOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER,
             Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testPutIfAbsentOnNonOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER,
             Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testPutIfAbsentOnNonOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, null, true);
   }

   public void testPutIfAbsentOnNonOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, null, false);
   }

   public void testReplaceOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testReplaceOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testReplaceOnPrimaryOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, null, true);
   }

   public void testReplaceOnPrimaryOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.BACKUP, null, false);
   }

   public void testReplaceOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceOnNonOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testReplaceOnNonOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testReplaceOnNonOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, null, true);
   }

   public void testReplaceOnNonOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE, Ownership.NON_OWNER, null, false);
   }

   public void testReplaceIfOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceIfOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceIfOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testReplaceIfOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testReplaceIfOnPrimaryOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, null, true);
   }

   public void testReplaceIfOnPrimaryOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.BACKUP, null, false);
   }

   public void testReplaceIfOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceIfOnNonOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD,
             false);
   }

   public void testReplaceIfOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testReplaceIfOnNonOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testReplaceIfOnNonOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, null, true);
   }

   public void testReplaceIfOnNonOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, null, false);
   }

   public void testRemoveIfOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testRemoveIfOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testRemoveIfOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testRemoveIfOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testRemoveIfOnPrimaryOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, null, true);
   }

   public void testRemoveIfOnPrimaryOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.BACKUP, null, false);
   }

   public void testRemoveIfOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testRemoveIfOnNonOwnerWithSkipCacheLoader() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD,
             false);
   }

   public void testRemoveIfOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             true);
   }

   public void testRemoveIfOnNonOwnerWithIgnoreReturnValues() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES,
             false);
   }

   public void testRemoveIfOnNonOwnerShared() {
      doTest(SHARED_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, null, true);
   }

   public void testRemoveIfOnNonOwner() {
      doTest(PRIVATE_STORE_CACHE_NAME, ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, null, false);
   }

   protected enum ConditionalOperation {
      PUT_IF_ABSENT {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.putIfAbsent(key, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value2 : value1;
         }
      },
      REPLACE {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.replace(key, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : value2;
         }
      },
      REPLACE_IF {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.replace(key, value1, value2);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : value2;
         }
      },
      REMOVE_IF {
         @Override
         public <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2) {
            cache.remove(key, value1);
         }

         @Override
         public <V> V finalValue(V value1, V value2, boolean skipLoad) {
            return skipLoad ? value1 : null;
         }
      };

      public abstract <K, V> void execute(Cache<K, V> cache, K key, V value1, V value2);

      public abstract <V> V finalValue(V value1, V value2, boolean skipLoad);
   }

   protected static class CacheHelper<K, V> {
      private final Map<Ownership, Cache<K, V>> cacheEnumMap;

      private CacheHelper() {
         cacheEnumMap = new EnumMap<>(Ownership.class);
      }

      public boolean addCache(Ownership ownership, Cache<K, V> cache) {
         boolean contains = cacheEnumMap.containsKey(ownership);
         if (!contains) {
            cacheEnumMap.put(ownership, cache);
         }
         return !contains;
      }

      private Cache<K, V> cache(Ownership ownership) {
         return cacheEnumMap.get(ownership);
      }

      private DummyInMemoryStore cacheStore(Ownership ownership) {
         Cache<K, V> cache = cache(ownership);
         return cache != null ? getFirstStore(cache) : null;
      }

      protected long loads(Ownership ownership) {
         Cache<K, V> cache = cache(ownership);
         if (cache == null) return 0;
         AsyncInterceptorChain chain = extractComponent(cache, AsyncInterceptorChain.class);
         CacheLoaderInterceptor interceptor = chain.findInterceptorExtending(CacheLoaderInterceptor.class);
         return interceptor.getCacheLoaderLoads();
      }

      private void resetStats(Ownership ownership) {
         Cache<K, V> cache = cache(ownership);
         if (cache == null) return;
         AsyncInterceptorChain chain = extractComponent(cache, AsyncInterceptorChain.class);
         CacheLoaderInterceptor interceptor = chain.findInterceptorExtending(
               CacheLoaderInterceptor.class);
         interceptor.resetStatistics();
      }
   }

}
