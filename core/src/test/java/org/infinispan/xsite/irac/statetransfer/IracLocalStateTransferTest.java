package org.infinispan.xsite.irac.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.xsite.AbstractXSiteTest;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.ManualIracManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tests the backup sending while topology change happens in the local cluster.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "irac.statetransfer.IracLocalStateTransferTest")
public class IracLocalStateTransferTest extends AbstractXSiteTest {

   private static final String LON = "LON-1";
   private static final String NYC = "NYC-2";
   private static final int NUM_NODES = 3;
   private final ControlledConsistentHashFactory<?> lonCHF = new ControlledConsistentHashFactory.Default(0, 1);
   private final ControlledConsistentHashFactory<?> nycCHF = new ControlledConsistentHashFactory.Default(0, 1);

   private TxMode lonTxMode;

   @Factory
   public Object[] factory() {
      List<IracLocalStateTransferTest> tests = new LinkedList<>();
      for (TxMode lon : TxMode.values()) {
         tests.add(new IracLocalStateTransferTest().setLonTxMode(lon));
      }
      return tests.toArray();
   }

   public void testStateTransfer(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);

      assertOwnership(key, 0);

      cache(LON, 0).put(key, value);

      IracMetadata metadata = extractMetadataFromPrimaryOwner(key);

      assertEventuallyInSite(NYC, cache -> value.equals(cache.get(key)), 30, TimeUnit.SECONDS);

      changeOwnership(LON, NUM_NODES); //primary owner changes from node-0 to node-3
      addNewNode(site(LON));
      site(LON).waitForClusterToForm(null);

      assertOwnership(key, 3);

      assertInDataContainer(LON, key, value, metadata);
   }

   public void testBackupSendAfterPrimaryFail(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);

      assertOwnership(key, 0);

      ManualIracManager iracManager = ManualIracManager.wrapCache(cache(LON, 0));
      iracManager.enable();

      cache(LON, 0).put(key, value);
      IracMetadata metadata = extractMetadataFromPrimaryOwner(key);

      assertInSite(NYC, cache -> assertNull(cache.get(key)));

      site(LON).kill(0); //kill the primary owner
      site(LON).waitForClusterToForm(null);

      assertEventuallyInSite(NYC, cache -> value.equals(cache.get(key)), 30, TimeUnit.SECONDS);

      assertInDataContainer(LON, key, value, metadata);
      assertInDataContainer(NYC, key, value, metadata);
   }

   public void testBackupRemovedKeySendAfterPrimaryFail(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);

      assertOwnership(key, 0);
      cache(LON, 0).put(key, value);
      assertEventuallyInSite(NYC, cache -> value.equals(cache.get(key)), 30, TimeUnit.SECONDS);

      ManualIracManager iracManager = ManualIracManager.wrapCache(cache(LON, 0));
      iracManager.enable(); //disable sending

      cache(LON, 0).remove(key);

      assertInSite(NYC, cache -> assertEquals(value, cache.get(key)));

      site(LON).kill(0); //kill the primary owner
      site(LON).waitForClusterToForm(null);

      assertEventuallyInSite(NYC, cache -> cache.get(key) == null, 30, TimeUnit.SECONDS);

      assertNotInDataContainer(LON, key);
      assertNotInDataContainer(NYC, key);
   }

   public void testNewPrimarySend(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);

      assertOwnership(key, 0);

      //this is the old primary owner.
      //it won't send the data. the new primary owner (node-3) will send it.
      ManualIracManager iracManager = ManualIracManager.wrapCache(cache(LON, 0));
      iracManager.enable();

      cache(LON, 0).put(key, value);
      IracMetadata metadata = extractMetadataFromPrimaryOwner(key);

      assertInSite(NYC, cache -> assertNull(cache.get(key)));

      changeOwnership(LON, NUM_NODES); //primary owner changes from node-0 to node-3
      addNewNode(site(LON));
      site(LON).waitForClusterToForm(null);

      assertEventuallyInSite(NYC, cache -> value.equals(cache.get(key)), 30, TimeUnit.SECONDS);

      assertInDataContainer(LON, key, value, metadata);
      assertInDataContainer(NYC, key, value, metadata);
   }

   public void testNewPrimarySendRemovedKey(Method method) {
      String key = TestingUtil.k(method);
      String value = TestingUtil.v(method);

      assertOwnership(key, 0);
      cache(LON, 0).put(key, value);
      assertEventuallyInSite(NYC, cache -> value.equals(cache.get(key)), 30, TimeUnit.SECONDS);

      //this is the old primary owner.
      //it won't send the data. the new primary owner (node-3) will send it.
      ManualIracManager iracManager = ManualIracManager.wrapCache(cache(LON, 0));
      iracManager.enable();

      cache(LON, 0).remove(key);

      assertInSite(NYC, cache -> assertEquals(value, cache.get(key)));

      changeOwnership(LON, NUM_NODES); //primary owner changes from node-0 to node-3
      addNewNode(site(LON));
      site(LON).waitForClusterToForm(null);

      assertEventuallyInSite(NYC, cache -> cache.get(key) == null, 30, TimeUnit.SECONDS);

      assertNotInDataContainer(LON, key);
      assertNotInDataContainer(NYC, key);
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      super.createBeforeMethod();

      //reset the consistent hash to default ownership
      changeOwnership(LON, 0);
      changeOwnership(NYC, 0);

      //reset the number of nodes
      for (TestSite site : sites) {
         int numNodes = site.cacheManagers().size();
         if (numNodes > NUM_NODES) {
            removeExtraNodes(site);
         } else if (numNodes < NUM_NODES) {
            addMissingNodes(site);
         }
      }

      //reset IracManager
      resetIracManager(LON);
      resetIracManager(NYC);
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"LON"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{lonTxMode};
   }

   @Override
   protected void createSites() {
      GlobalConfigurationBuilder lonGCB = globalConfigurationBuilderForSite();
      TestSite lon = addSite(LON);
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getLonActiveConfig();
         lon.addCache(lonGCB, builder);
      }

      GlobalConfigurationBuilder nycGCB = globalConfigurationBuilderForSite();
      TestSite nyc = addSite(NYC);
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getNycActiveConfig();
         nyc.addCache(nycGCB, builder);
      }

      lon.waitForClusterToForm(null);
      nyc.waitForClusterToForm(null);
   }

   private IracLocalStateTransferTest setLonTxMode(TxMode txMode) {
      this.lonTxMode = txMode;
      return this;
   }

   private void resetIracManager(String site) {
      for (Cache<String, String> cache : this.<String, String>caches(site)) {
         IracManager manager = TestingUtil.extractComponent(cache, IracManager.class);
         if (manager instanceof ManualIracManager) {
            ((ManualIracManager) manager).disable(ManualIracManager.DisableMode.DROP);
         }
      }
   }

   private void assertOwnership(String key, int primaryOwner) {
      assertTrue(getDistributionForKey(cache(LON, primaryOwner), key).isPrimary());
      assertTrue(getDistributionForKey(cache(LON, 1), key).isWriteBackup());
   }

   private void assertInDataContainer(String site, String key, String value, IracMetadata metadata) {
      for (Cache<String, String> cache : this.<String, String>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         InternalDataContainer<String, String> dc = getInternalDataContainer(cache);
         InternalCacheEntry<String, String> ice = dc.peek(key);
         log.debugf("Checking DataContainer in %s. entry=%s", DistributionTestHelper.addressOf(cache), ice);
         assertNotNull(String.format("Internal entry is null for key %s", key), ice);
         assertEquals("Internal entry wrong key", key, ice.getKey());
         assertEquals("Internal entry wrong value", value, ice.getValue());
         assertEquals("Internal entry wrong metadata", metadata, ice.getInternalMetadata().iracMetadata());
      }
   }

   private void assertNotInDataContainer(String site, String key) {
      for (Cache<String, String> cache : this.<String, String>caches(site)) {
         if (isNotWriteOwner(cache, key)) {
            continue;
         }
         InternalDataContainer<String, String> dc = getInternalDataContainer(cache);
         InternalCacheEntry<String, String> ice = dc.peek(key);
         log.debugf("Checking DataContainer in %s. entry=%s", DistributionTestHelper.addressOf(cache), ice);
         assertNull(String.format("Internal entry found for key %s", key), ice);
      }
   }

   private boolean isNotWriteOwner(Cache<String, String> cache, String key) {
      return !getDistributionForKey(cache, key).isWriteOwner();
   }

   private IracMetadata extractMetadataFromPrimaryOwner(String key) {
      Cache<String, String> cache = findPrimaryOwner(key);
      InternalDataContainer<String, String> dataContainer = getInternalDataContainer(cache);
      InternalCacheEntry<String, String> entry = dataContainer.peek(key);
      assertNotNull(entry);
      PrivateMetadata internalMetadata = entry.getInternalMetadata();
      assertNotNull(internalMetadata);
      IracMetadata metadata = internalMetadata.iracMetadata();
      assertNotNull(metadata);
      return metadata;

   }

   private InternalDataContainer<String, String> getInternalDataContainer(Cache<String, String> cache) {
      //noinspection unchecked
      return TestingUtil.extractComponent(cache, InternalDataContainer.class);
   }

   private Cache<String, String> findPrimaryOwner(String key) {
      for (Cache<String, String> c : this.<String, String>caches(LON)) {
         if (getDistributionForKey(c, key).isPrimary()) {
            return c;
         }
      }
      throw new IllegalStateException(String.format("Unable to find primary owner for key %s", key));
   }

   private DistributionInfo getDistributionForKey(Cache<String, String> cache, String key) {
      return TestingUtil.extractComponent(cache, ClusteringDependentLogic.class)
            .getCacheTopology()
            .getDistribution(key);
   }

   private void removeExtraNodes(TestSite site) {
      int numNodes = site.cacheManagers().size();
      while (numNodes > NUM_NODES) {
         site.kill(--numNodes);
      }
      site.waitForClusterToForm(null);
   }


   private void addMissingNodes(TestSite site) {
      int numNodes = site.cacheManagers().size();
      while (numNodes < NUM_NODES) {
         addNewNode(site);
         ++numNodes;
      }
      site.waitForClusterToForm(null);
   }


   private void addNewNode(TestSite site) {
      String siteName = site.getSiteName();
      GlobalConfigurationBuilder gBuilder = globalConfigurationBuilderForSite();
      ConfigurationBuilder builder = LON.equals(siteName) ? getLonActiveConfig() : getNycActiveConfig();
      site.addCache(gBuilder, builder);
   }

   private void changeOwnership(String site, int primaryOwner) {
      ControlledConsistentHashFactory<?> chf = LON.equals(site) ? lonCHF : nycCHF;
      chf.setOwnerIndexes(primaryOwner, 1);
   }

   private GlobalConfigurationBuilder globalConfigurationBuilderForSite() {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializers(TestDataSCI.INSTANCE, ControlledConsistentHashFactory.SCI.INSTANCE);
      return builder;
   }

   private ConfigurationBuilder getNycActiveConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash()
            .consistentHashFactory(nycCHF)
            .numSegments(1);
      return builder;
   }

   private ConfigurationBuilder getLonActiveConfig() {
      ConfigurationBuilder builder = lonTxMode.create();
      BackupConfigurationBuilder lonBackupConfigurationBuilder = builder.sites().addBackup();
      lonBackupConfigurationBuilder
            .site(NYC)
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      builder.clustering().hash()
            .consistentHashFactory(lonCHF)
            .numSegments(1);
      return builder;
   }

   private enum TxMode {
      NON_TX {
         @Override
         ConfigurationBuilder create() {
            return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
         }
      },
      OPT_TX {//2PC with Versions

         @Override
         ConfigurationBuilder create() {
            ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
            return builder;
         }
      },
      PES_TX { // 1PC

         @Override
         ConfigurationBuilder create() {
            ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
            builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
            return builder;
         }
      };

      abstract ConfigurationBuilder create();
   }
}
