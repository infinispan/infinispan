package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstWriter;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.Ownership;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.SkipException;
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
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC })
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
      builder.jmxStatistics().enable();
      builder.persistence()
            .passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + (shared ? "-shared" : storePrefix))
            .fetchPersistentState(false)
            .purgeOnStartup(true)
            .shared(shared);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return builder;
   }

   private static <K, V> void writeToStore(CacheHelper<K, V> cacheHelper, Ownership ownership, K key, V value) {
      cacheHelper.cacheStore(ownership).write(marshalledEntry(key, value, cacheHelper.marshaller(ownership)));
   }

   private static <K, V> MarshalledEntry<K, V> marshalledEntry(K key, V value, StreamingMarshaller marshaller) {
      return new MarshalledEntryImpl<>(key, value, null, marshaller);
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
            assertTrue(cacheHelper.addCache(Ownership.NON_OWNER, cache) || cacheMode.isScattered());
         }
      }
      return cacheHelper;
   }

   private void doTest(List<Cache<String, String>> cacheList, ConditionalOperation operation, Ownership ownership,
                       Flag flag, boolean shared) {
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
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC), 3);

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
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testPutIfAbsentOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testPutIfAbsentOnPrimaryOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, null, true);
   }

   public void testPutIfAbsentOnPrimaryOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testPutIfAbsentOnBackupOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.BACKUP, null, false);
   }

   public void testPutIfAbsentOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testPutIfAbsentOnNonOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testPutIfAbsentOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testPutIfAbsentOnNonOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testPutIfAbsentOnNonOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, null, true);
   }

   public void testPutIfAbsentOnNonOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.PUT_IF_ABSENT, Ownership.NON_OWNER, null, false);
   }

   public void testReplaceOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testReplaceOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testReplaceOnPrimaryOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, null, true);
   }

   public void testReplaceOnPrimaryOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceOnBackupOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.BACKUP, null, false);
   }

   public void testReplaceOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceOnNonOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testReplaceOnNonOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testReplaceOnNonOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, null, true);
   }

   public void testReplaceOnNonOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE, Ownership.NON_OWNER, null, false);
   }

   public void testReplaceIfOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceIfOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceIfOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testReplaceIfOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testReplaceIfOnPrimaryOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, null, true);
   }

   public void testReplaceIfOnPrimaryOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testReplaceIfOnBackupOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.BACKUP, null, false);
   }

   public void testReplaceIfOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testReplaceIfOnNonOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testReplaceIfOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testReplaceIfOnNonOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testReplaceIfOnNonOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, null, true);
   }

   public void testReplaceIfOnNonOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REPLACE_IF, Ownership.NON_OWNER, null, false);
   }

   public void testRemoveIfOnPrimaryOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testRemoveIfOnPrimaryOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testRemoveIfOnPrimaryOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testRemoveIfOnPrimaryOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testRemoveIfOnPrimaryOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, null, true);
   }

   public void testRemoveIfOnPrimaryOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.PRIMARY, null, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.SKIP_CACHE_LOAD, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, Flag.IGNORE_RETURN_VALUES, false);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, null, true);
   }

   @InCacheMode(CacheMode.DIST_SYNC)
   public void testRemoveIfOnBackupOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.BACKUP, null, false);
   }

   public void testRemoveIfOnNonOwnerWithSkipCacheLoaderShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, true);
   }

   public void testRemoveIfOnNonOwnerWithSkipCacheLoader() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.SKIP_CACHE_LOAD, false);
   }

   public void testRemoveIfOnNonOwnerWithIgnoreReturnValuesShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, true);
   }

   public void testRemoveIfOnNonOwnerWithIgnoreReturnValues() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, Flag.IGNORE_RETURN_VALUES, false);
   }

   public void testRemoveIfOnNonOwnerShared() {
      doTest(this.caches(SHARED_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, null, true);
   }

   public void testRemoveIfOnNonOwner() {
      doTest(this.caches(PRIVATE_STORE_CACHE_NAME), ConditionalOperation.REMOVE_IF, Ownership.NON_OWNER, null, false);
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
         return cache != null ? getFirstWriter(cache) : null;
      }

      private StreamingMarshaller marshaller(Ownership ownership) {
         return cacheEnumMap.get(ownership).getAdvancedCache().getComponentRegistry().getCacheMarshaller();
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
