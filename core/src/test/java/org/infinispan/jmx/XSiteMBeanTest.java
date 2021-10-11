package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.test.TestDataSCI;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.irac.ManualIracManager;
import org.infinispan.xsite.spi.AlwaysRemoveXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.DefaultXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test for cross-site JMX attributes.
 *
 * @author Pedro Ruivo
 * @author Durgesh Anaokar
 * @since 13.0
 */
@Test(groups = "functional", testName = "jmx.XSiteMBeanTest")
public class XSiteMBeanTest extends AbstractMultipleSitesTest {

   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 1;

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private final List<ManualIracManager> iracManagerList;
   private final List<ManualRpcManager> rpcManagerList;
   private final List<BlockingInterceptor<IracPutKeyValueCommand>> blockingInterceptorList;

   public XSiteMBeanTest() {
      this.iracManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
      this.rpcManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
      this.blockingInterceptorList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
   }

   private static void assertSameAttributeAndOperation(MBeanServer mBeanServer, ObjectName objectName,
         Attribute attribute, String site) throws Exception {
      long val1 = invokeLongAttribute(mBeanServer, objectName, attribute);
      long val2 = invokeLongOperation(mBeanServer, objectName, attribute, site);
      log.debugf("%s op(%s) = %d", objectName, attribute, val2);
      assertEquals("Wrong value for " + attribute, val1, val2);
   }

   private static void assertAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute,
         long expected) throws Exception {
      long val = invokeLongAttribute(mBeanServer, objectName, attribute);
      assertEquals("Wrong attribute value for " + attribute, expected, val);
   }

   private static void eventuallyAssertAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute) {
      Supplier<Long> s = () -> {
         try {
            return invokeLongAttribute(mBeanServer, objectName, attribute);
         } catch (RuntimeException e) {
            throw e;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      };
      eventuallyEquals("Wrong attribute " + attribute, 1L, s);
   }

   private static void assertHasAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute)
         throws Exception {
      long val = invokeLongAttribute(mBeanServer, objectName, attribute);
      if (val == -1L) {
         fail("Attribute " + attribute + " expected to be different. " + val + " == -1");
      }
   }

   private static void assertOperation(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute, String site,
         long expected) throws Exception {
      long val = invokeLongOperation(mBeanServer, objectName, attribute, site);
      log.debugf("%s op(%s) = %d", objectName, attribute, val);
      assertEquals("Wrong operation value for " + attribute, expected, val);
   }

   private static long invokeLongOperation(MBeanServer mBeanServer, ObjectName rpcManager, Attribute attribute,
         String siteName)
         throws Exception {
      Object val = mBeanServer
            .invoke(rpcManager, attribute.operationName, new Object[]{siteName}, new String[]{String.class.getName()});
      assertTrue(val instanceof Number);
      return ((Number) val).longValue();
   }

   private static long invokeLongAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute)
         throws Exception {
      Object val = mBeanServer.getAttribute(objectName, attribute.attributeName);
      log.debugf("%s attr(%s) = %d", objectName, attribute, val);
      assertTrue(val instanceof Number);
      return ((Number) val).longValue();
   }

   private static int invokeQueueSizeAttribute(MBeanServer mBeanServer, ObjectName objectName) throws Exception {
      Object val = mBeanServer.getAttribute(objectName, Attribute.QUEUE_SIZE.attributeName);
      assertTrue(val instanceof Number);
      return ((Number) val).intValue();
   }

   private static ManualRpcManager wrapRpcManager(Cache<?, ?> cache) {
      RpcManager rpcManager = extractComponent(cache, RpcManager.class);
      if (rpcManager instanceof ManualRpcManager) {
         return (ManualRpcManager) rpcManager;
      }
      return wrapComponent(cache, RpcManager.class, ManualRpcManager::new);
   }

   public void testRequestsSent(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method);
      Cache<String, String> cache = cache(0, 0);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager = getRpcManagerObjectName(0);

      assertTrue(mBeanServer.isRegistered(rpcManager));
      resetRpcManagerStats(mBeanServer, rpcManager);

      cache.put(k(method), v(method));
      assertEventuallyInSite(siteName(1), cache1 -> Objects.equals(value, cache1.get(key)), 10, TimeUnit.SECONDS);

      // the metrics are updated after the reply, so wait until the reply is received and the metrics updated
      awaitUnitKeysSent();
      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_SENT, 1);
      assertOperation(mBeanServer, rpcManager, Attribute.REQ_SENT, siteName(1), 1);

      assertHasAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME);
      assertHasAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME);
      assertHasAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME);

      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.MIN_TIME, siteName(1));
      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.AVG_TIME, siteName(1));
      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.MAX_TIME, siteName(1));

      // we only have 1 request, so min==max==avg
      assertEquals(invokeLongAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME),
            invokeLongAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME));
      assertEquals(invokeLongAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME),
            invokeLongAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME));

      resetRpcManagerStats(mBeanServer, rpcManager);
   }

   public void testRequestsReceived(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method);
      Cache<String, String> cache = cache(0, 0);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager = getRpcManagerObjectName(1);

      assertTrue(mBeanServer.isRegistered(rpcManager));
      resetRpcManagerStats(mBeanServer, rpcManager);

      cache.put(k(method), v(method));
      assertEventuallyInSite(siteName(1), cache1 -> Objects.equals(value, cache1.get(key)), 10, TimeUnit.SECONDS);

      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_RECV, 1);
      assertOperation(mBeanServer, rpcManager, Attribute.REQ_RECV, siteName(0), 1);

      resetRpcManagerStats(mBeanServer, rpcManager);
   }

   public void testQueueSizeStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager = getIracManagerObjectName(0);
      assertTrue(mBeanServer.isRegistered(iracManager));

      assertEquals(0, invokeQueueSizeAttribute(mBeanServer, iracManager));

      ManualRpcManager rpcManager = rpcManagerList.get(0);

      // block request in RpcManager so the queue is not flush right away.
      BlockedRequest req = rpcManager.block();

      cache(0, 0).put("key", "value");

      // request is blocked, check the queue size
      req.awaitRequest();
      assertEquals(1, invokeQueueSizeAttribute(mBeanServer, iracManager));

      // let the request proceed
      rpcManager.unblock();
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value", cache.get("key")));

      // the queue is clean after the reply is received. wait until it is.
      awaitUnitKeysSent();
      assertEquals(0, invokeQueueSizeAttribute(mBeanServer, iracManager));

      setStatisticsEnabled(mBeanServer, iracManager, false);
      assertEquals(-1, invokeQueueSizeAttribute(mBeanServer, iracManager));
      setStatisticsEnabled(mBeanServer, iracManager, true);
   }

   public void testNumberOfConflictsStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getIracManagerObjectName(0);
      ObjectName iracManager2 = getIracManagerObjectName(1);
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      assertAttribute(mBeanServer, iracManager1, Attribute.CONFLICTS, 0);
      assertAttribute(mBeanServer, iracManager2, Attribute.CONFLICTS, 0);

      createConflict(false);

      // make sure the conflict is seen
      eventuallyAssertAttribute(mBeanServer, iracManager1, Attribute.CONFLICTS);
      eventuallyAssertAttribute(mBeanServer, iracManager2, Attribute.CONFLICTS);

      assertAttribute(mBeanServer, iracManager1, Attribute.CONFLICT_LOCAL, 1);
      assertAttribute(mBeanServer, iracManager2, Attribute.CONFLICT_LOCAL, 0);

      assertAttribute(mBeanServer, iracManager1, Attribute.CONFLICT_REMOTE, 0);
      assertAttribute(mBeanServer, iracManager2, Attribute.CONFLICT_REMOTE, 1);

      assertAttribute(mBeanServer, iracManager1, Attribute.CONFLICT_MERGED, 0);
      assertAttribute(mBeanServer, iracManager2, Attribute.CONFLICT_MERGED, 0);

      // now reset statistics
      for (ObjectName objectName : Arrays.asList(iracManager1, iracManager2)) {
         resetIracManagerStats(mBeanServer, objectName);
         setStatisticsEnabled(mBeanServer, objectName, false);
         assertAttribute(mBeanServer, objectName, Attribute.CONFLICTS, -1);
         assertAttribute(mBeanServer, objectName, Attribute.CONFLICT_LOCAL, -1);
         assertAttribute(mBeanServer, objectName, Attribute.CONFLICT_REMOTE, -1);
         setStatisticsEnabled(mBeanServer, objectName, true);
      }
   }

   public void testNumberOfConflictMerged() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager1 = getIracManagerObjectName(0);
      ObjectName iracManager2 = getIracManagerObjectName(1);
      assertTrue(mBeanServer.isRegistered(iracManager1));
      assertTrue(mBeanServer.isRegistered(iracManager2));

      // replace merge policy to generate a new value
      replaceComponent(cache(0, 0), XSiteEntryMergePolicy.class, AlwaysRemoveXSiteEntryMergePolicy.getInstance(), true);
      replaceComponent(cache(1, 0), XSiteEntryMergePolicy.class, AlwaysRemoveXSiteEntryMergePolicy.getInstance(), true);

      assertAttribute(mBeanServer, iracManager1, Attribute.CONFLICT_MERGED, 0);
      assertAttribute(mBeanServer, iracManager2, Attribute.CONFLICT_MERGED, 0);

      createConflict(true);

      // make sure the conflict is seen
      eventuallyAssertAttribute(mBeanServer, iracManager1, Attribute.CONFLICT_MERGED);
      eventuallyAssertAttribute(mBeanServer, iracManager2, Attribute.CONFLICT_MERGED);

      // put back the default merge policy
      replaceComponent(cache(0, 0), XSiteEntryMergePolicy.class, DefaultXSiteEntryMergePolicy.getInstance(), true);
      replaceComponent(cache(1, 0), XSiteEntryMergePolicy.class, DefaultXSiteEntryMergePolicy.getInstance(), true);

      //reset stats
      for (ObjectName objectName : Arrays.asList(iracManager1, iracManager2)) {
         resetIracManagerStats(mBeanServer, objectName);
         setStatisticsEnabled(mBeanServer, objectName, false);
         assertAttribute(mBeanServer, objectName, Attribute.CONFLICT_MERGED, -1);
         setStatisticsEnabled(mBeanServer, objectName, true);
      }
   }

   public void testNumberOfDiscardsStats() throws Throwable {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName iracManager = getIracManagerObjectName(1);
      assertTrue(mBeanServer.isRegistered(iracManager));

      assertAttribute(mBeanServer, iracManager, Attribute.DISCARDS, 0);

      createDiscard();

      assertAttribute(mBeanServer, iracManager, Attribute.DISCARDS, 1);

      // now reset statistics
      resetIracManagerStats(mBeanServer, iracManager);
      setStatisticsEnabled(mBeanServer, iracManager, false);
      assertAttribute(mBeanServer, iracManager, Attribute.DISCARDS, -1);
      setStatisticsEnabled(mBeanServer, iracManager, true);
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      for (int i = 0; i < N_SITES; ++i) {
         if (i == siteIndex) {
            //don't add our site as backup.
            continue;
         }
         builder.sites()
                .addBackup()
                .site(siteName(i))
                .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      }
      builder.statistics().enable();
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      builder.cacheContainer()
             .statistics(true)
             .jmx().enable().domain("xsite-mbean-" + siteIndex).mBeanServerLookup(mBeanServerLookup)
            .metrics().accurateSize(true);
      return builder;
   }

   @Override
   protected void afterSitesCreated() {
      for (int i = 0; i < N_SITES; ++i) {
         for (Cache<?, ?> cache : caches(siteName(i))) {
            rpcManagerList.add(wrapRpcManager(cache));
            iracManagerList.add(ManualIracManager.wrapCache(cache));
            BlockingInterceptor<IracPutKeyValueCommand> interceptor = new BlockingInterceptor<>(new CyclicBarrier(2), IracPutKeyValueCommand.class, false, false);
            interceptor.suspend(true);
            blockingInterceptorList.add(interceptor);
            //noinspection deprecation
            cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptor(interceptor, 0);
         }
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      rpcManagerList.forEach(ManualRpcManager::unblock);
      iracManagerList.forEach(m -> m.disable(ManualIracManager.DisableMode.DROP));
      blockingInterceptorList.forEach(i -> i.suspend(true));
      super.clearContent();
   }

   private void awaitUnitKeysSent() {
      // we assume 1 node per site!
      assertEquals(1, defaultNumberOfNodes());
      ManualIracManager iracManager = iracManagerList.get(0);
      eventually(iracManager::isEmpty);
   }

   private void createConflict(boolean isConflictMerged) {
      final String key = "conflict-key";
      cache(0, 0).put(key, "value1");
      // make sure all sites received the key
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value1", cache.get(key)));

      // disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);
      blockingInterceptorList.forEach(i -> i.suspend(false));

      cache(0, 0).put(key, "v-2");
      cache(1, 0).put(key, "v-3");

      // enable xsite. this will send the keys!
      iracManagerList.forEach(manualIracManager -> manualIracManager.disable(ManualIracManager.DisableMode.SEND));

      // wait until command is received.
      blockingInterceptorList.forEach(i -> {
         try {
            i.proceed();
         } catch (RuntimeException e) {
            throw e;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });
      blockingInterceptorList.forEach(i -> i.suspend(true));
      // let the command go
      blockingInterceptorList.forEach(i -> {
         try {
            i.proceed();
         } catch (RuntimeException e) {
            throw e;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      });

      if (isConflictMerged) {
         eventuallyAssertInAllSitesAndCaches(cache -> Objects.isNull(cache.get(key)));
      } else {
         eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("v-2", cache.get(key)));
      }
   }

   private void createDiscard() throws Throwable {
      // the discard is "simulated" by doing a state transfer.
      // The state transfer sends everything to the remote site.
      // The remote site ignores since it contains the same version.
      final String key = "discard-key";
      cache(0, 0).put(key, "value1");
      // make sure all sites received the key
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value1", cache.get(key)));
      extractComponent(cache(0, 0), XSiteStateTransferManager.class).startPushState(siteName(1));
      assertEventuallyInSite(siteName(0),
            cache -> extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty(), 10,
            TimeUnit.SECONDS);
   }

   private void resetRpcManagerStats(MBeanServer mBeanServer, ObjectName rpcManager) throws Exception {
      mBeanServer.invoke(rpcManager, "resetStatistics", new Object[0], new String[0]);
      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_SENT, 0);
      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_RECV, 0);
      assertAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME, -1);
      assertAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME, -1);
      assertAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME, -1);
      for (int i = 0; i < N_SITES; ++i) {
         String site = siteName(i);
         assertOperation(mBeanServer, rpcManager, Attribute.REQ_SENT, site, 0);
         assertOperation(mBeanServer, rpcManager, Attribute.REQ_RECV, site, 0);
         assertOperation(mBeanServer, rpcManager, Attribute.MIN_TIME, site, -1);
         assertOperation(mBeanServer, rpcManager, Attribute.AVG_TIME, site, -1);
         assertOperation(mBeanServer, rpcManager, Attribute.MAX_TIME, site, -1);
      }
   }

   private void resetIracManagerStats(MBeanServer mBeanServer, ObjectName iracManager) throws Exception {
      mBeanServer.invoke(iracManager, "resetStatistics", new Object[0], new String[0]);
      assertAttribute(mBeanServer, iracManager, Attribute.CONFLICTS, 0);
      assertAttribute(mBeanServer, iracManager, Attribute.CONFLICT_LOCAL, 0);
      assertAttribute(mBeanServer, iracManager, Attribute.CONFLICT_REMOTE, 0);
      assertAttribute(mBeanServer, iracManager, Attribute.CONFLICT_MERGED, 0);
      assertAttribute(mBeanServer, iracManager, Attribute.DISCARDS, 0);
   }

   private void setStatisticsEnabled(MBeanServer mBeanServer, ObjectName objectName, boolean enabled) throws Exception {
      mBeanServer.setAttribute(objectName, new javax.management.Attribute("StatisticsEnabled", enabled));
   }

   private String getJmxDomain(int siteIndex) {
      return manager(siteIndex, 0).getCacheManagerConfiguration().jmx().domain();
   }

   private ObjectName getRpcManagerObjectName(int siteIndex) {
      return getCacheObjectName(getJmxDomain(siteIndex), getDefaultCacheName() + "(dist_sync)", "RpcManager");
   }

   private ObjectName getIracManagerObjectName(int siteIndex) {
      return getCacheObjectName(getJmxDomain(siteIndex), getDefaultCacheName() + "(dist_sync)", "AsyncXSiteStatistics");
   }

   private enum Attribute {
      REQ_SENT("NumberXSiteRequests", "NumberXSiteRequestsSentTo"),
      REQ_RECV("NumberXSiteRequestsReceived", "NumberXSiteRequestsReceivedFrom"),
      AVG_TIME("AverageXSiteReplicationTime", "AverageXSiteReplicationTimeTo"),
      MAX_TIME("MaximumXSiteReplicationTime", "MaximumXSiteReplicationTimeTo"),
      MIN_TIME("MinimumXSiteReplicationTime", "MinimumXSiteReplicationTimeTo"),
      QUEUE_SIZE("QueueSize", null),
      CONFLICTS("NumberOfConflicts", null),
      CONFLICT_LOCAL("NumberOfConflictsLocalWins", null),
      CONFLICT_REMOTE("NumberOfConflictsRemoteWins", null),
      CONFLICT_MERGED("NumberOfConflictsMerged", null),
      DISCARDS("NumberOfDiscards", null);

      final String attributeName;
      final String operationName;

      Attribute(String attributeName, String operationName) {
         this.attributeName = attributeName;
         this.operationName = operationName;
      }
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

   private static class DummyXsiteResponse<O> extends CompletableFuture<O> implements XSiteResponse<O>,
         XSiteResponse.XSiteResponseCompleted {

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
