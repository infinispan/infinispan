package org.infinispan.xsite.statetransfer.failures;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Helper methods for x-site state transfer during topology changes.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class AbstractTopologyChangeTest extends AbstractTwoSitesTest {

   protected static final int NR_KEYS = 20; //10 * chunk size

   protected AbstractTopologyChangeTest() {
      this.implicitBackupCache = true;
      this.cleanup = CleanupPhase.AFTER_METHOD;
      this.initialClusterSize = 3;
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

   protected static ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   protected void awaitLocalStateTransfer(String site) {
      log.debugf("Await until rebalance in site '%s' is finished!", site);
      assertEventuallyInSite(site, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return !extractComponent(cache, StateConsumer.class).isStateTransferInProgress() &&
                  !extractComponent(cache, StateProvider.class).isStateTransferInProgress();
         }
      }, 30, TimeUnit.SECONDS);
   }

   protected void awaitXSiteStateSent(String site) {
      log.debugf("Await until all nodes in '%s' has sent the state!", site);
      assertEventuallyInSite(site, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty();
         }
      }, 30, TimeUnit.SECONDS);
   }

   protected void awaitXSiteStateReceived(String site) {
      log.debugf("Await until all nodes in '%s' has received the state!", site);
      assertEventuallyInSite(site, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return !extractComponent(cache, CommitManager.class).isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER);
         }
      }, 30, TimeUnit.SECONDS);
   }

   protected Future<Void> triggerTopologyChange(final String siteName, final int removeIndex) {
      if (removeIndex >= 0) {
         return fork(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               log.debugf("Shutting down cache %s", addressOf(cache(siteName, removeIndex)));
               site(siteName).kill(removeIndex);
               log.debugf("Wait for cluster to form on caches %s", site(siteName).getCaches(null));
               site(siteName).waitForClusterToForm(null, 60, TimeUnit.SECONDS);
               return null;
            }
         });
      } else {
         log.debug("Adding new cache");
         site(siteName).addCache(globalConfigurationBuilderForSite(siteName), lonConfigurationBuilder());
         return fork(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               log.debugf("Wait for cluster to form on caches %s", site(siteName).getCaches(null));
               site(siteName).waitForClusterToForm(null, 60, TimeUnit.SECONDS);
               return null;
            }
         });
      }
   }

   protected void initBeforeTest() {
      takeSiteOffline(LON, NYC);
      assertOffline(LON, NYC);
      putData();
      assertDataInSite(LON);
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertTrue(cache.isEmpty());
         }
      });
   }

   protected void putData() {
      for (int i = 0; i < NR_KEYS; ++i) {
         cache(LON, 0).put(key(Integer.toString(i)), val(Integer.toString(i)));
      }
   }

   protected void assertData() {
      assertDataInSite(LON);
      assertDataInSite(NYC);
   }

   protected void startStateTransfer(Cache<?, ?> coordinator, String toSite) {
      XSiteAdminOperations operations = extractComponent(coordinator, XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.pushState(toSite));
   }

   protected void takeSiteOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.SUCCESS, operations.takeSiteOffline(remoteSite));
   }

   protected void assertOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.OFFLINE, operations.siteStatus(remoteSite));
   }

   protected void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.ONLINE, operations.siteStatus(remoteSite));
   }

   protected void assertDataInSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < NR_KEYS; ++i) {
               assertEquals(val(Integer.toString(i)), cache.get(key(Integer.toString(i))));
            }
         }
      });
   }

   protected <K, V> TestCaches<K, V> createTestCache(TopologyEvent topologyEvent, String siteName) {
      switch (topologyEvent) {
         case JOIN:
            return new TestCaches<>(this.<K, V>cache(LON, 0), this.<K, V>cache(siteName, 0), -1);
         case COORDINATOR_LEAVE:
            return new TestCaches<>(this.<K, V>cache(LON, 1), this.<K, V>cache(siteName,0), 1);
         case LEAVE:
            return new TestCaches<>(this.<K, V>cache(LON, 0), this.<K, V>cache(siteName, 0), 1);
         case SITE_MASTER_LEAVE:
            return new TestCaches<>(this.<K, V>cache(LON, 1), this.<K, V>cache(siteName, 1), 0);
         default:
            //make sure we select the caches
            throw new IllegalStateException();
      }
   }

   protected void printTestCaches(TestCaches<?, ?> testCaches) {
      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s",
                 addressOf(testCaches.controllerCache),
                 addressOf(testCaches.coordinator),
                 testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));
   }

   protected static enum TopologyEvent {
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
      public final Cache<K, V> controllerCache;
      public final int removeIndex;

      public TestCaches(Cache<K, V> coordinator, Cache<K, V> controllerCache, int removeIndex) {
         this.coordinator = coordinator;
         this.controllerCache = controllerCache;
         this.removeIndex = removeIndex;
      }
   }

}
