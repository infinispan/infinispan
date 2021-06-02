package org.infinispan.xsite.irac.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.xsite.AbstractXSiteTest;
import org.infinispan.xsite.irac.ControlledIracVersionGenerator;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.ManualIracManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test for {@link IracManager} to check if can load and send the update even if the key is only in the cache store.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "xsite.irac.persistence.IracMetadataStoreTest")
public class IracMetadataStoreTest extends AbstractXSiteTest {

   private static final String LON = "LON-1";
   private static final String NYC = "NYC-2";
   private static final int NUM_NODES = 3;

   private static final AtomicLong V_GENERATOR = new AtomicLong(0);
   private final List<Runnable> cleanupTask = Collections.synchronizedList(new LinkedList<>());
   private TxMode lonTxMode;
   private TxMode nycTxMode;
   private boolean passivation;

   private static ConfigurationBuilder createConfigurationBuilder(TxMode txMode, boolean passivation) {
      ConfigurationBuilder builder;
      switch (txMode) {
         case NON_TX:
            builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
            break;
         case OPT_TX:
            builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            break;
         case PES_TX:
            builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
            break;
         default:
            throw new IllegalStateException();
      }
      builder.persistence().passivation(passivation);
      builder.clustering().hash().numSegments(4);
      return builder;
   }

   private static IracMetadata generateNew() {
      long v = V_GENERATOR.incrementAndGet();
      return new IracMetadata(LON, new IracEntryVersion(Collections.singletonMap(LON, new TopologyIracVersion(1, v))));
   }

   private static ManualIracVersionGenerator createManualIracVerionGenerator(Cache<String, Object> cache) {
      return TestingUtil.wrapComponent(cache, IracVersionGenerator.class, ManualIracVersionGenerator::new);
   }

   @Factory
   public Object[] factory() {
      List<IracMetadataStoreTest> tests = new LinkedList<>();
      for (TxMode lon : TxMode.values()) {
         for (TxMode nyc : TxMode.values()) {
            tests.add(new IracMetadataStoreTest().setLonTxMode(lon).setNycTxMode(nyc).setPassivation(true));
            tests.add(new IracMetadataStoreTest().setLonTxMode(lon).setNycTxMode(nyc).setPassivation(false));
         }
      }
      return tests.toArray();
   }

   public void testSendEvictedKey(Method method) {
      final String key = TestingUtil.k(method, 1);
      final Cache<String, Object> pOwnerCache = findPrimaryOwner(key);
      final ManualIracVersionGenerator vGenerator = createManualIracVerionGenerator(pOwnerCache);
      final ManualIracManager iracManager = createManualIracManager(pOwnerCache);

      IracMetadata metadata = generateNew();
      vGenerator.metadata = metadata; //next write will have this version

      pOwnerCache.put(key, "v1");
      evictKey(LON, key); //key only exists in cache store


      assertNotInDataContainer(LON, key);
      assertInCacheStore(LON, key, "v1", metadata);

      assertInSite(NYC, cache -> assertNull(cache.get(key)));

      iracManager.sendKeys(); //test if send can fetch the key from persistence

      assertEventuallyInSite(NYC, cache -> cache.get(key) != null, 30, TimeUnit.SECONDS);

      assertInDataContainer(NYC, key, "v1", metadata);
      if (!passivation) {
         assertInCacheStore(NYC, key, "v1", metadata);
      }
   }

   public void testCorrectMetadataStored(Method method) {
      final String key = TestingUtil.k(method, 1);
      final Cache<String, Object> pOwnerCache = findPrimaryOwner(key);
      final ManualIracVersionGenerator vGenerator = createManualIracVerionGenerator(pOwnerCache);
      final ManualIracManager iracManager = createManualIracManager(pOwnerCache);

      IracMetadata metadata = generateNew();
      vGenerator.metadata = metadata; //next write will have this version

      pOwnerCache.put(key, "v");

      assertInDataContainer(LON, key, "v", metadata);
      if (!passivation) {
         assertInCacheStore(LON, key, "v", metadata);
      }

      assertInSite(NYC, cache -> assertNull(cache.get(key)));

      iracManager.sendKeys();

      assertEventuallyInSite(NYC, cache -> cache.get(key) != null, 30, TimeUnit.SECONDS);

      assertInDataContainer(NYC, key, "v", metadata);
      if (!passivation) {
         assertInCacheStore(NYC, key, "v", metadata);
      }
   }

   public void testKeyEvictedOnReceive(Method method) {
      final String key = TestingUtil.k(method, 1);
      final Cache<String, Object> pOwnerCache = findPrimaryOwner(key);
      final ManualIracVersionGenerator vGenerator = createManualIracVerionGenerator(pOwnerCache);
      final ManualIracManager iracManager = createManualIracManager(pOwnerCache);

      IracMetadata metadata = generateNew();
      vGenerator.metadata = metadata; //next write will have this version

      pOwnerCache.put(key, "v2");
      iracManager.sendKeys();

      assertInDataContainer(LON, key, "v2", metadata);
      if (!passivation) {
         assertInCacheStore(LON, key, "v2", metadata);
      }

      assertEventuallyInSite(NYC, cache -> cache.get(key) != null, 30, TimeUnit.SECONDS);

      assertInDataContainer(NYC, key, "v2", metadata);
      if (!passivation) {
         assertInCacheStore(NYC, key, "v2", metadata);
      }

      //in this test, NYC primary-owner should be able to fetch the IracMetadata from cache loader to perform validation
      evictKey(NYC, key);

      assertNotInDataContainer(NYC, key);
      assertInCacheStore(NYC, key, "v2", metadata);

      metadata = generateNew();
      vGenerator.metadata = metadata;

      pOwnerCache.put(key, "v3");
      iracManager.sendKeys();

      assertEventuallyInSite(NYC, cache -> "v3".equals(cache.get(key)), 30, TimeUnit.SECONDS);

      assertInDataContainer(NYC, key, "v3", metadata);
      if (!passivation) {
         assertInCacheStore(NYC, key, "v3", metadata);
      }
   }

   public void testPreload(Method method) {
      final String key = TestingUtil.k(method, 1);
      final Cache<String, Object> pOwnerCache = findPrimaryOwner(key);
      final ManualIracVersionGenerator vGenerator = createManualIracVerionGenerator(pOwnerCache);
      final ManualIracManager iracManager = createManualIracManager(pOwnerCache);

      IracMetadata metadata = generateNew();
      vGenerator.metadata = metadata; //next write will have this version

      //we evict the key and then invoke the preload() method
      //to avoid killing and starting a node.

      pOwnerCache.put(key, "v4");
      iracManager.sendKeys();
      assertEventuallyInSite(NYC, cache -> "v4".equals(cache.get(key)), 30, TimeUnit.SECONDS);
      evictKey(LON, key);

      assertNotInDataContainer(LON, key);
      assertInCacheStore(LON, key, "v4", metadata);

      preload();

      assertInDataContainer(LON, key, "v4", metadata);
      if (!passivation) {
         assertInCacheStore(LON, key, "v4", metadata);
      }
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      cleanupTask.forEach(Runnable::run);
      super.destroy();
   }

   @Override
   protected void createSites() {
      GlobalConfigurationBuilder lonGCB = globalConfigurationBuilderForSite();
      TestSite lon = addSite(LON);
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getLonActiveConfig();
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true);
         lon.addCache(lonGCB, builder);
      }

      GlobalConfigurationBuilder nycGCB = globalConfigurationBuilderForSite();
      TestSite nyc = addSite(NYC);
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getNycActiveConfig();
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true);
         nyc.addCache(nycGCB, builder);
      }

      lon.waitForClusterToForm(null);
      nyc.waitForClusterToForm(null);
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"LON", "NYC", "passivation"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{lonTxMode, nycTxMode, passivation};
   }

   private void preload() {
      for (Cache<String, String> cache : this.<String, String>caches(LON)) {
         PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
         pm.preload().toCompletableFuture().join();
      }
   }

   private GlobalConfigurationBuilder globalConfigurationBuilderForSite() {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      return builder;
   }

   private ConfigurationBuilder getNycActiveConfig() {
      return createConfigurationBuilder(nycTxMode, passivation);
   }

   private ConfigurationBuilder getLonActiveConfig() {
      ConfigurationBuilder builder = createConfigurationBuilder(lonTxMode, passivation);
      BackupConfigurationBuilder lonBackupConfigurationBuilder = builder.sites().addBackup();
      lonBackupConfigurationBuilder
            .site(NYC)
            .strategy(BackupConfiguration.BackupStrategy.ASYNC)
            .sites().addInUseBackupSite(NYC);
      return builder;
   }

   private ManualIracManager createManualIracManager(Cache<String, Object> cache) {
      ManualIracManager manager = ManualIracManager.wrapCache(cache);
      manager.enable();
      cleanupTask.add(manager::stop);
      return manager;
   }

   private void assertNotInDataContainer(String site, String key) {
      for (Cache<String, Object> cache : this.<String, Object>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         InternalDataContainer<String, Object> dc = getInternalDataContainer(cache);
         InternalCacheEntry<String, Object> ice = dc.peek(key);
         log.debugf("Checking DataContainer in %s. entry=%s", DistributionTestHelper.addressOf(cache), ice);
         assertNull(String.format("Internal entry found for key %s", key), ice);
      }
   }

   private void assertInDataContainer(String site, String key, String value, IracMetadata metadata) {
      for (Cache<String, Object> cache : this.<String, Object>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         InternalDataContainer<String, Object> dc = getInternalDataContainer(cache);
         InternalCacheEntry<String, Object> ice = dc.peek(key);
         log.debugf("Checking DataContainer in %s. entry=%s", DistributionTestHelper.addressOf(cache), ice);
         assertNotNull(String.format("Internal entry is null for key %s", key), ice);
         assertEquals("Internal entry wrong key", key, ice.getKey());
         assertEquals("Internal entry wrong value", value, ice.getValue());
         assertEquals("Internal entry wrong metadata", metadata, ice.getInternalMetadata().iracMetadata());
      }
   }

   private void assertInCacheStore(String site, String key, String value, IracMetadata metadata) {
      for (Cache<String, Object> cache : this.<String, Object>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         WaitNonBlockingStore<String, Object> cl = TestingUtil.getFirstStoreWait(cache);
         MarshallableEntry<String, Object> mEntry = cl.loadEntry(key);
         log.debugf("Checking CacheLoader in %s. entry=%s", DistributionTestHelper.addressOf(cache), mEntry);
         assertNotNull(String.format("CacheLoader entry is null for key %s", key), mEntry);
         assertEquals("CacheLoader entry wrong key", key, mEntry.getKey());
         assertEquals("CacheLoader entry wrong value", value, mEntry.getValue());
         assertNotNull("CacheLoader entry wrong internal metadata", mEntry.getInternalMetadata());
         assertEquals("CacheLoader entry wrong IRAC metadata", metadata, mEntry.getInternalMetadata().iracMetadata());
      }
   }

   private InternalDataContainer<String, Object> getInternalDataContainer(Cache<String, Object> cache) {
      //noinspection unchecked
      return TestingUtil.extractComponent(cache, InternalDataContainer.class);
   }

   private void evictKey(String site, String key) {
      for (Cache<String, Object> cache : this.<String, Object>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         getInternalDataContainer(cache).evict(getSegmentForKey(cache, key), key).toCompletableFuture().join();
      }
   }

   private IracMetadataStoreTest setLonTxMode(TxMode lonTxMode) {
      this.lonTxMode = lonTxMode;
      return this;
   }

   private IracMetadataStoreTest setNycTxMode(TxMode nycTxMode) {
      this.nycTxMode = nycTxMode;
      return this;
   }

   private IracMetadataStoreTest setPassivation(boolean passivation) {
      this.passivation = passivation;
      return this;
   }

   private DistributionInfo getDistributionForKey(Cache<String, Object> cache, String key) {
      return TestingUtil.extractComponent(cache, ClusteringDependentLogic.class)
            .getCacheTopology()
            .getDistribution(key);
   }

   private int getSegmentForKey(Cache<String, Object> cache, String key) {
      return getDistributionForKey(cache, key).segmentId();
   }

   private Cache<String, Object> findPrimaryOwner(String key) {
      for (Cache<String, Object> c : this.<String, Object>caches(LON)) {
         if (getDistributionForKey(c, key).isPrimary()) {
            return c;
         }
      }
      throw new IllegalStateException(String.format("Unable to find primary owner for key %s", key));
   }

   private boolean isNotWriteOwner(Cache<String, Object> cache, String key) {
      return !getDistributionForKey(cache, key).isWriteOwner();
   }

   private enum TxMode {
      NON_TX,
      OPT_TX, //2PC with Versions
      PES_TX // 1PC
   }

   private static class ManualIracVersionGenerator extends ControlledIracVersionGenerator {

      private volatile IracMetadata metadata;

      public ManualIracVersionGenerator(IracVersionGenerator actual) {
         super(actual);
      }

      @Override
      public IracMetadata generateNewMetadata(int segment) {
         return metadata;
      }
   }
}
