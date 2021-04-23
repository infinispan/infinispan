package org.infinispan.xsite.irac;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.spi.AlwaysRemoveXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.DefaultXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteMergePolicy;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Function test for {@link XsiteStatistics}.
 *
 * @author Durgesh Anaokar
 * @since 13.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracXsiteStatsTest")
public class IracXsiteStatsTest extends AbstractMultipleSitesTest {
   private static final String DOMAIN_NAME = IracXsiteStatsTest.class.getSimpleName();
   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 1;
   private final List<ManualIracManager> iracManagerList;
   private final List<ManualRpcManager> rpcManagerList;
   protected final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   protected IracXsiteStatsTest() {
      this.iracManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
      this.rpcManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
   }

   public static ManualRpcManager wrapCache(Cache<?, ?> cache) {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      if (rpcManager instanceof ManualRpcManager) {
         return (ManualRpcManager) rpcManager;
      }
      return TestingUtil.wrapComponent(cache, RpcManager.class, ManualRpcManager::new);
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   public void testQueueSizeStats(Method method) throws Exception {

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));

      int queueSize = (Integer) mBeanServer.getAttribute(iracManager1, "queueSize");
      assertEquals(queueSize, (int) 0);

      // The initial state transfer uses cache commands, so it also increases the
      ManualRpcManager rpcManager = rpcManagerList.get(0);
      BlockedRequest req = rpcManager.block();

      cache(0, 0).put("key", "value");

      req.awaitRequest();
      assertEquals((int) mBeanServer.getAttribute(iracManager1, "queueSize"), 1);

      rpcManager.unblock();
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value", cache.get("key")));

      assertEquals((int) mBeanServer.getAttribute(iracManager1, "queueSize"), 0);

   }

   public void testNoOfConflictsStats() throws Exception {

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      ObjectName iracManager2 = getCacheObjectName(DOMAIN_NAME + 10, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      // The initial state transfer uses cache commands, so it also increases the
      long noOfConflicts = (Long) mBeanServer.getAttribute(iracManager1, "NoOfConflicts");

      assertEquals(noOfConflicts, (long) 0);

      createConflict(Boolean.FALSE);

      noOfConflicts = (Long) mBeanServer.getAttribute(iracManager2, "NoOfConflicts");
      assertEquals(noOfConflicts, (long) 1);

      // now reset statistics
      mBeanServer.invoke(iracManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager1, "noOfConflicts"), (long) 0);

      mBeanServer.invoke(iracManager2, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager2, "noOfConflicts"), (long) 0);

      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.FALSE));
      assertEquals(mBeanServer.getAttribute(iracManager1, "noOfConflicts"), (long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.TRUE));

   }

   public void testNumberOfConflictLocalWins() throws Exception {

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      ObjectName iracManager2 = getCacheObjectName(DOMAIN_NAME + 10, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      long numberOfConflictLocalWins = (Long) mBeanServer.getAttribute(iracManager1, "numberOfConflictLocalWins");

      assertEquals(numberOfConflictLocalWins, (long) 0);

      createConflict(Boolean.FALSE);

      numberOfConflictLocalWins = (Long) mBeanServer.getAttribute(iracManager1, "numberOfConflictLocalWins");

      assertEquals(numberOfConflictLocalWins, 1);

      // now reset statistics
      mBeanServer.invoke(iracManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictLocalWins"), (long) 0);

      mBeanServer.invoke(iracManager2, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager2, "numberOfConflictLocalWins"), (long) 0);

      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.FALSE));

      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictLocalWins"), (long) -1);
      // reset stats enabled parameter
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.TRUE));

   }

   public void testNumberOfConflictRemoteWins() throws Exception {

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      ObjectName iracManager2 = getCacheObjectName(DOMAIN_NAME + 10, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      long numberOfConflictRemoteWins = (Long) mBeanServer.getAttribute(iracManager2, "numberOfConflictRemoteWins");

      assertEquals(numberOfConflictRemoteWins, (long) 0);

      createConflict(Boolean.FALSE);

      numberOfConflictRemoteWins = (Long) mBeanServer.getAttribute(iracManager2, "numberOfConflictRemoteWins");
      assertEquals(numberOfConflictRemoteWins, (long) 1);

      // now reset statistics
      mBeanServer.invoke(iracManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictRemoteWins"), (long) 0);

      mBeanServer.invoke(iracManager2, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager2, "numberOfConflictRemoteWins"), (long) 0);
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.FALSE));
      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictRemoteWins"), (long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.TRUE));

   }

   public void testNumberOfConflictMerged() throws Exception {

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      ObjectName iracManager2 = getCacheObjectName(DOMAIN_NAME + 10, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      TestingUtil.replaceComponent(cache(0, 0), XSiteEntryMergePolicy.class,
            AlwaysRemoveXSiteEntryMergePolicy.getInstance(), true);
      TestingUtil.replaceComponent(cache(1, 0), XSiteEntryMergePolicy.class,
            AlwaysRemoveXSiteEntryMergePolicy.getInstance(), true);

      // do test, i.e. create a conflict and check the metric

      long numberOfConflictMerged = (Long) mBeanServer.getAttribute(iracManager1, "numberOfConflictMerged");

      assertEquals(numberOfConflictMerged, (long) 0);

      createConflict(Boolean.TRUE);

      numberOfConflictMerged = (Long) mBeanServer.getAttribute(iracManager2, "numberOfConflictMerged");
      assertEquals(numberOfConflictMerged, (long) 1);

      TestingUtil.replaceComponent(cache(0, 0), XSiteEntryMergePolicy.class,
            DefaultXSiteEntryMergePolicy.getInstance(), true);
      TestingUtil.replaceComponent(cache(1, 0), XSiteEntryMergePolicy.class,
            DefaultXSiteEntryMergePolicy.getInstance(), true);

      // now reset statistics
      mBeanServer.invoke(iracManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictMerged"), (long) 0);

      mBeanServer.invoke(iracManager2, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(iracManager2, "numberOfConflictMerged"), (long) 0);

      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.FALSE));

      assertEquals(mBeanServer.getAttribute(iracManager1, "numberOfConflictMerged"), (long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.TRUE));

   }

   public void testNoOfDiscardsStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getCacheObjectName(DOMAIN_NAME + 0, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      ObjectName iracManager2 = getCacheObjectName(DOMAIN_NAME + 10, getDefaultCacheName() + "(dist_sync)",
            "XSiteStatistics");
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      // The initial state transfer uses cache commands, so it also increases the
      long noOfDiscards = (Long) mBeanServer.getAttribute(iracManager1, "noOfDiscards");
      assertEquals(noOfDiscards, (long) 0);

      createDiscard();

      noOfDiscards = (Long) mBeanServer.getAttribute(iracManager2, "noOfDiscards");
      assertEquals(noOfDiscards, (long) 1);
      // now reset statistics
      mBeanServer.invoke(iracManager1, "resetStatistics", new Object[0], new String[0]);

      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.FALSE));

      assertEquals(mBeanServer.getAttribute(iracManager1, "noOfDiscards"), (long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(iracManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
      mBeanServer.setAttribute(iracManager2, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.sites().mergePolicy(XSiteMergePolicy.DEFAULT);
      for (int i = 0; i < N_SITES; ++i) {
         if (i == siteIndex) {
            // don't add our site as backup.
            continue;
         }
         builder.sites().addBackup().site(siteName(i)).strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .statistics().enable();
      }
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.cacheContainer().statistics(true).jmx().enabled(true).mBeanServerLookup(mBeanServerLookup);
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE).jmx().enable()
            .domain(siteIndex < 1 ? DOMAIN_NAME : DOMAIN_NAME + siteIndex);
      return builder;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      rpcManagerList.forEach(ManualRpcManager::unblock);
      iracManagerList.forEach(iracmanger -> iracmanger.disable(ManualIracManager.DisableMode.DROP));
      super.clearContent();
   }

   @Override
   protected void afterSitesCreated() {
      for (int i = 0; i < N_SITES; ++i) {
         for (Cache<?, ?> cache : caches(siteName(i))) {
            rpcManagerList.add(wrapCache(cache));
            iracManagerList.add(ManualIracManager.wrapCache(cache));
         }
      }
   }

   private void createConflict(Boolean isConflictMerged) {
      final String key = "conflict-key";
      cache(0, 0).put(key, "value1");
      // make sure all sites received the key
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value1", cache.get(key)));

      // disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);

      cache(0, 0).put(key, "v-2");
      cache(1, 0).put(key, "v-3");

      // enable xsite. this will send the keys!
      iracManagerList.forEach(manualIracManager -> manualIracManager.disable(ManualIracManager.DisableMode.SEND));

      if (isConflictMerged)
         eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(null, cache.get(key)));
      else
         eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("v-2", cache.get(key)));

   }

   private void createDiscard() {
      final String key = "discard-key";
      cache(0, 0).put(key, "value1");
      // make sure all sites received the key
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value1", cache.get(key)));
      try {
         TestingUtil.extractComponent(cache(0, 0), XSiteStateTransferManager.class).startPushState(siteName(1));
      } catch (Throwable throwable) {
         log.xsiteAdminOperationError("pushState", siteName(1), throwable);
      }
      assertEventuallyInSite(siteName(0), cache -> TestingUtil
            .extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty(), 10,
            TimeUnit.SECONDS);
   }

   private static class ManualRpcManager extends AbstractDelegatingRpcManager {

      private volatile BlockedRequest blockedRequest;

      public ManualRpcManager(RpcManager realOne) {
         super(realOne);
      }

      @Override
      public <O> XSiteResponse<O> invokeXSite(XSiteBackup backup, XSiteReplicateCommand<O> command) {
         BlockedRequest req = blockedRequest;

         if (req != null) {
            DummyXsiteResponse<O> rsp = new DummyXsiteResponse<>();
            req.thenRun(() -> rsp.onRequest(super.invokeXSite(backup, command)));
            return rsp;
         } else {
            return super.invokeXSite(backup, command);
         }

      }

      BlockedRequest block() {
         BlockedRequest req = new BlockedRequest();
         blockedRequest = req;
         return req;
      }

      void unblock() {
         BlockedRequest req = blockedRequest;

         if (req != null) {
            req.continueRequest();
            blockedRequest = null;
         }
      }

   }

   private static class BlockedRequest extends CompletableFuture<Void> {
      private final CountDownLatch latch;

      private BlockedRequest() {
         latch = new CountDownLatch(1);
      }

      @Override
      public CompletableFuture<Void> thenRun(Runnable action) {
         latch.countDown();
         return super.thenRun(action);
      }

      void awaitRequest() throws InterruptedException {
         AssertJUnit.assertTrue(latch.await(10, TimeUnit.SECONDS));
      }

      void continueRequest() {
         complete(null);
      }

   }

   private static class DummyXsiteResponse<O> extends CompletableFuture<O>
         implements XSiteResponse<O>, XSiteResponse.XSiteResponseCompleted {

      private volatile XSiteResponse<O> realOne;
      private volatile XSiteBackup backup;
      private volatile long sendTimeStamp;
      private volatile long durationNanos;

      @Override
      public void onCompleted(XSiteBackup backup, long sendTimeNanos, long durationNanos, Throwable throwable) {
         this.backup = backup;
         this.sendTimeStamp = sendTimeNanos;
         this.durationNanos = durationNanos;
         if (throwable != null) {
            completeExceptionally(throwable);
         } else {
            complete(realOne.toCompletableFuture().join());
         }
      }

      @Override
      public void whenCompleted(XSiteResponseCompleted xSiteResponseCompleted) {
         this.whenComplete((ignore, throwable) -> xSiteResponseCompleted.onCompleted(backup, sendTimeStamp,
               durationNanos, throwable));
      }

      void onRequest(XSiteResponse<O> response) {

         realOne = response;
         response.whenCompleted(this);
      }

   }

}
