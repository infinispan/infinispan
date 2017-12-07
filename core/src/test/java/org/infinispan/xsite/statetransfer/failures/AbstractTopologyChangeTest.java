package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferManager.STATUS_ERROR;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.xsite.statetransfer.AbstractStateTransferTest;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;

/**
 * Helper methods for x-site state transfer during topology changes.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class AbstractTopologyChangeTest extends AbstractStateTransferTest {

   private static final int NR_KEYS = 20; //10 * chunk size

   AbstractTopologyChangeTest() {
      this.implicitBackupCache = true;
      this.cleanup = CleanupPhase.AFTER_METHOD;
      this.initialClusterSize = 3;
   }

   private static ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   void awaitLocalStateTransfer(String site) {
      log.debugf("Await until rebalance in site '%s' is finished!", site);
      assertEventuallyInSite(site, cache -> !extractComponent(cache, StateConsumer.class).isStateTransferInProgress() &&
            !extractComponent(cache, StateProvider.class).isStateTransferInProgress(), 30, TimeUnit.SECONDS);
   }

   void awaitXSiteStateSent(String site) {
      log.debugf("Await until all nodes in '%s' has sent the state!", site);
      assertEventuallyInSite(site,
            cache -> extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty(), 30,
            TimeUnit.SECONDS);
   }

   Future<Void> triggerTopologyChange(final String siteName, final int removeIndex) {
      if (removeIndex >= 0) {
         return fork(() -> {
            log.debugf("Shutting down cache %s", addressOf(cache(siteName, removeIndex)));
            site(siteName).kill(removeIndex);
            log.debugf("Wait for cluster to form on caches %s", site(siteName).getCaches(null));
            site(siteName).waitForClusterToForm(null, 60, TimeUnit.SECONDS);
            return null;
         });
      } else {
         log.debug("Adding new cache");
         site(siteName).addCache(globalConfigurationBuilderForSite(siteName), lonConfigurationBuilder());
         return fork(() -> {
            log.debugf("Wait for cluster to form on caches %s", site(siteName).getCaches(null));
            site(siteName).waitForClusterToForm(null, 60, TimeUnit.SECONDS);
            return null;
         });
      }
   }

   void initBeforeTest() {
      takeSiteOffline();
      assertOffline();
      putData();
      assertDataInSite(LON);
      assertInSite(NYC, cache -> assertTrue(cache.isEmpty()));
   }

   void assertData() {
      assertDataInSite(LON);
      assertDataInSite(NYC);
   }

   void assertDataInSite(String siteName) {
      assertInSite(siteName, cache -> {
         for (int i = 0; i < NR_KEYS; ++i) {
            assertEquals(val(Integer.toString(i)), cache.get(key(Integer.toString(i))));
         }
      });
   }

   void assertXSiteErrorStatus() {
      assertEquals(STATUS_ERROR, getXSitePushStatus());
   }

   String getXSitePushStatus() {
      return adminOperations().getPushStateStatus().get(NYC);
   }

   <K, V> TestCaches<K, V> createTestCache(TopologyEvent topologyEvent, String siteName) {
      switch (topologyEvent) {
         case JOIN:
            return new TestCaches<>(this.cache(LON, 0), this.cache(siteName, 0), -1);
         case COORDINATOR_LEAVE:
            return new TestCaches<>(this.cache(LON, 1), this.cache(siteName, 0), 1);
         case LEAVE:
            return new TestCaches<>(this.cache(LON, 0), this.cache(siteName, 0), 1);
         case SITE_MASTER_LEAVE:
            return new TestCaches<>(this.cache(LON, 1), this.cache(siteName, 1), 0);
         default:
            //make sure we select the caches
            throw new IllegalStateException();
      }
   }

   void printTestCaches(TestCaches<?, ?> testCaches) {
      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s",
            addressOf(testCaches.controllerCache),
            addressOf(testCaches.coordinator),
            testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      builder.stateTransfer().chunkSize(2).timeout(2000);
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return createConfiguration();
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return createConfiguration();
   }

   private void putData() {
      for (int i = 0; i < NR_KEYS; ++i) {
         cache(LON, 0).put(key(Integer.toString(i)), val(Integer.toString(i)));
      }
   }

   protected enum TopologyEvent {
      /**
       * Some node joins the cluster.
       */
      JOIN,
      /**
       * Some non-important node (site master neither the coordinator) leaves.
       */
      LEAVE,
      /**
       * Site master leaves.
       */
      SITE_MASTER_LEAVE,
      /**
       * X-Site state transfer coordinator leaves.
       */
      COORDINATOR_LEAVE
   }

   protected static class TestCaches<K, V> {
      public final Cache<K, V> coordinator;
      final Cache<K, V> controllerCache;
      final int removeIndex;

      TestCaches(Cache<K, V> coordinator, Cache<K, V> controllerCache, int removeIndex) {
         this.coordinator = coordinator;
         this.controllerCache = controllerCache;
         this.removeIndex = removeIndex;
      }
   }

}
