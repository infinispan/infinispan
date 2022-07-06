package org.infinispan.xsite.offline;

import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ExponentialBackOff;
import org.infinispan.xsite.AbstractXSiteTest;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the async cross-site replication is working properly.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "functional", testName = "xsite.offline.AsyncOfflineTest")
public class AsyncOfflineTest extends AbstractXSiteTest {

   private static final int NUM_NODES = 3;
   private static final int NUM_FAILURES = 6;

   private static final String LON = "LON-1";
   private static final String NYC = "NYC-2";
   private static final String SFO = "SFO-3";

   public void testSFOOffline(Method method) {
      String cacheName = method.getName();
      defineCache(LON, cacheName, getLONConfiguration());
      defineCache(NYC, cacheName, getNYCOrSFOConfiguration());

      for (int i = 0; i < NUM_NODES; ++i) {
         iracManager(LON, cacheName, i).setBackOff(ExponentialBackOff.NO_OP);
      }

      String key = method.getName() + "-key";
      int primaryOwner = primaryOwnerIndex(cacheName, key);
      for (int i = 0; i < NUM_NODES; ++i) {
         //probably and overkill, but this will test on primary owner, backup owner, sitemaster and non-sitemaster
         doTestInNode(cacheName, i, primaryOwner, key);
      }
   }

   public void testSlowSFO(Method method) {
      createTestSite(SFO);

      String cacheName = method.getName();
      defineCache(LON, cacheName, getLONConfiguration());
      defineCache(NYC, cacheName, getNYCOrSFOConfiguration());
      defineCache(SFO, cacheName, getNYCOrSFOConfiguration());

      for (int i = 0; i < NUM_NODES; ++i) {
         iracManager(LON, cacheName, i).setBackOff(ExponentialBackOff.NO_OP);
      }

      String key = method.getName() + "-key";
      int primaryOwner = primaryOwnerIndex(cacheName, key);

      cache(LON, cacheName, 0).put("key", "value");
      eventuallyEquals("value", () -> cache(SFO, cacheName, 0).get("key"));

      assertEquals(0, takeOfflineManager(LON, cacheName, primaryOwner).getOfflineStatus(SFO).getFailureCount());

      List<DiscardInboundHandler> handlers = replaceSFOInboundHandler();
      handlers.forEach(h -> h.discard = true);

      //same as testSFOOffline but every command should fail with timeout for SFO.
      for (int i = 0; i < NUM_NODES; ++i) {
         //probably and overkill, but this will test on primary owner, backup owner, sitemaster and non-sitemaster
         doTestInNode(cacheName, i, primaryOwner, key);
      }
   }

   public void testReset(Method method) {
      createTestSite(SFO);

      String cacheName = method.getName();
      defineCache(LON, cacheName, getLONConfiguration());
      defineCache(NYC, cacheName, getNYCOrSFOConfiguration());
      defineCache(SFO, cacheName, getNYCOrSFOConfiguration());

      for (int i = 0; i < NUM_NODES; ++i) {
         iracManager(LON, cacheName, i).setBackOff(ExponentialBackOff.NO_OP);
      }

      String key = method.getName() + "-key";
      int primaryOwner = primaryOwnerIndex(cacheName, key);

      Cache<String, String> lonCache = cache(LON, cacheName, 0);
      Cache<String, String> sfoCache = cache(SFO, cacheName, 0);
      OfflineStatus lonStatus = takeOfflineManager(LON, cacheName, primaryOwner).getOfflineStatus(SFO);

      lonCache.put(key, "value");
      eventuallyEquals("value", () -> sfoCache.get(key));
      assertEquals(0, lonStatus.getFailureCount());

      List<DiscardInboundHandler> handlers = replaceSFOInboundHandler();
      handlers.forEach(h -> h.discard = true);

      //SFO request should timeout (timeout=1 sec) and it retries (max retries = 6)
      lonCache.put(key, "value2");
      eventuallyEquals(1, lonStatus::getFailureCount);
      assertEquals("value", sfoCache.get(key));

      handlers.forEach(h -> h.discard = false);

      // make sure the key is flushed. we don't want to receive timeouts after this point
      eventually(() -> {
         for (int i = 0; i < NUM_NODES; ++i) {
            if (!iracManager(LON, cacheName, i).isEmpty()) {
               return false;
            }
         }
         return true;
      });

      //SFO request will succeed and reset the take offline status
      lonCache.put(key, "value3");
      eventuallyEquals("value3", () -> sfoCache.get(key));
      eventuallyEquals(0, lonStatus::getFailureCount);
   }

   @AfterMethod(alwaysRun = true)
   public void killSFO() {
      killSite(SFO);
   }

   @Override
   protected void createSites() {
      //we have 3 sites: LON, NYC and SFO. SFO is offline.
      createTestSite(LON);
      createTestSite(NYC);
      waitForSites(LON, NYC);
   }

   private void doTestInNode(String cacheName, int index, int primaryOwnerIndex, String key) {
      Cache<String, String> cache = this.cache(LON, cacheName, index);
      assertOnline(cacheName, index, NYC);
      assertOnline(cacheName, index, SFO);

      if (index != primaryOwnerIndex) {
         assertOnline(cacheName, primaryOwnerIndex, NYC);
         assertOnline(cacheName, primaryOwnerIndex, SFO);
      }

      for (int i = 0; i < NUM_FAILURES; ++i) {
         cache.put(key, "value");
      }

      if (index == primaryOwnerIndex) {
         assertOnline(cacheName, index, NYC);
         assertEventuallyOffline(cacheName, index);
      } else {
         assertOnline(cacheName, index, NYC);
         assertOnline(cacheName, index, SFO);

         assertOnline(cacheName, primaryOwnerIndex, NYC);
         assertEventuallyOffline(cacheName, primaryOwnerIndex);
      }

      assertBringSiteOnline(cacheName, primaryOwnerIndex);
   }

   private void assertOnline(String cacheName, int index, String targetSiteName) {
      OfflineStatus status = takeOfflineManager(LON, cacheName, index).getOfflineStatus(targetSiteName);
      assertTrue(status.isEnabled());
      assertFalse("Site " + targetSiteName + " is offline. status=" + status, status.isOffline());
   }

   private void assertEventuallyOffline(String cacheName, int index) {
      OfflineStatus status = takeOfflineManager(LON, cacheName, index).getOfflineStatus(SFO);
      assertTrue(status.isEnabled());
      eventually(() -> "Site " + SFO + " is online. status=" + status, status::isOffline);
   }

   private void assertBringSiteOnline(String cacheName, int index) {
      OfflineStatus status = takeOfflineManager(LON, cacheName, index).getOfflineStatus(SFO);
      assertTrue("Unable to bring " + SFO + " online. status=" + status, status.bringOnline());
   }


   private int primaryOwnerIndex(String cacheName, String key) {
      for (int i = 0; i < NUM_NODES; ++i) {
         boolean isPrimary = extractCacheTopology(cache(LON, cacheName, i))
               .getDistribution(key)
               .isPrimary();
         if (isPrimary) {
            return i;
         }
      }
      throw new IllegalStateException();
   }

   private Configuration getLONConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash().numSegments(4);
      builder.sites().addBackup()
            .site(NYC)
            .backupFailurePolicy(BackupFailurePolicy.FAIL)
            .replicationTimeout(1000) //keep it small so that the test doesn't take long to run
            .takeOffline()
            .afterFailures(NUM_FAILURES)
            .backup()
            .strategy(BackupConfiguration.BackupStrategy.SYNC);

      builder.sites().addBackup()
            .site(SFO)
            .replicationTimeout(1000) //keep it small so that the test doesn't take long to run
            .takeOffline()
            .afterFailures(NUM_FAILURES)
            .backup()
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);

      return builder.build();
   }

   private Configuration getNYCOrSFOConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build();
   }

   private void defineCache(String siteName, String cacheName, Configuration configuration) {
      TestSite site = site(siteName);
      site.cacheManagers().get(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, configuration);
      site.waitForClusterToForm(cacheName);
   }

   private void createTestSite(String siteName) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      createSite(siteName, NUM_NODES, gcb, new ConfigurationBuilder());
   }

   private List<DiscardInboundHandler> replaceSFOInboundHandler() {
      List<DiscardInboundHandler> handlers = new ArrayList<>(NUM_NODES);
      for (EmbeddedCacheManager manager : site(SFO).cacheManagers()) {
         handlers.add(wrapGlobalComponent(manager, InboundInvocationHandler.class, DiscardInboundHandler::new, true));
      }
      return handlers;
   }

   private static class DiscardInboundHandler implements InboundInvocationHandler {

      private final InboundInvocationHandler handler;
      private volatile boolean discard;

      private DiscardInboundHandler(InboundInvocationHandler handler) {
         this.handler = handler;
         this.discard = false;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         handler.handleFromCluster(origin, command, reply, order);
      }

      @Override
      public void handleFromRemoteSite(String origin, XSiteReplicateCommand<?> command, Reply reply, DeliverOrder order) {
         if (discard) {
            return;
         }
         handler.handleFromRemoteSite(origin, command, reply, order);
      }
   }
}
