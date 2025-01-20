package org.infinispan.xsite.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.status.BringSiteOnlineResponse;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.infinispan.xsite.status.TakeSiteOfflineResponse;
import org.jgroups.protocols.relay.RELAY2;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test for cross-site automatic state transfer.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Test(groups = "functional", testName = "xsite.statetransfer.XSiteAutoStateTransferTest")
public class XSiteAutoStateTransferTest extends AbstractMultipleSitesTest {

   private final int nrKeys = defaultNumberOfNodes() * 5;
   private final List<Runnable> cleanupTasks = new CopyOnWriteArrayList<>();

   public void testSyncStrategyDoNotTriggerStateTransfer() throws InterruptedException {
      String remoteSite = siteName(2); //site2 is the sync one

      //make the remote site offline.
      takeSiteOffline(null, remoteSite);

      SiteMasterController controller = findSiteMaster(null);

      //block sites up event and wait until received
      controller.getStateTransferManager().startBlockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      // check the correct event and let it proceed
      controller.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite).run();

      //coordinator don't even query other nodes state (run on the same thread)
      controller.getRpcManager().expectNoCommand();

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i, null);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testManualModeDoNotTriggerStateTransfer() throws InterruptedException {
      String remoteSite = siteName(1); //site1 is the async one

      //make the remote site offline.
      takeSiteOffline(null, remoteSite);
      setAutoStateTransferMode(null, remoteSite, XSiteStateTransferMode.MANUAL);

      SiteMasterController controller = findSiteMaster(null);

      //block sites up event and wait until received
      controller.getStateTransferManager().startBlockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      // check the correct event and let it proceed
      controller.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite).run();

      //coordinator don't even query other nodes state (run on the same thread)
      controller.getRpcManager().expectNoCommand();

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i, null);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testSingleManualModeDoNotTriggerStateTransfer()
         throws InterruptedException, TimeoutException, ExecutionException {
      String remoteSite = siteName(1); //site1 is the async one

      //make the remote site offline.
      takeSiteOffline(null, remoteSite);
      setAutoStateTransferMode(null, remoteSite, XSiteStateTransferMode.AUTO);

      SiteMasterController controller = findSiteMaster(null);

      // having a single node set to manual should be enough to block the state transfer
      stateTransferManager((controller.managerIndex + 1) % defaultNumberOfNodes(), null).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.MANUAL);

      //block sites up event and wait until received
      controller.getStateTransferManager().startBlockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      //check if it is the correct event
      var continueEvent = controller.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite);

      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req = controller
            .getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);

      //let the event continue, it will be handled in this thread, which is fine
      continueEvent.run();

      //we expect the coordinator to send a command
      ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand> cmd = req.get(30, TimeUnit.SECONDS);
      cmd.send().receiveAll();

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i, null);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testAutoStateTransfer(Method method) throws InterruptedException, TimeoutException, ExecutionException {
      String remoteSite = siteName(1); //site1 is the async one

      //we need at least one node with offline status
      //we make all of them to put some data
      takeSiteOffline(null, remoteSite);
      takeSiteOffline(null, siteName(2));
      setAutoStateTransferMode(null, remoteSite, XSiteStateTransferMode.AUTO);

      //lets put some data
      insertDataInSite0(method, null);

      //make sure data didn't go through
      checkNoDataInSite1(method, null);

      //let the state command go through
      SiteMasterController controller = findSiteMaster(null, XSiteStatePushCommand.class,
            IracCleanupKeysCommand.class, IracTombstoneStateResponseCommand.class, StateTransferCancelCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req1 =
            controller.getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);
      // req2 removed, the site status is global and store in an internal cache
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteStateTransferStartSendCommand>> req3 =
            controller.getRpcManager().expectCommandAsync(XSiteStateTransferStartSendCommand.class);

      //block sites up event and wait until received
      controller.getStateTransferManager().startBlockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      // check the correct event and let it proceed
      controller.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite).run();

      //make sure the commands are blocked
      req1.get(10, TimeUnit.SECONDS).send().receiveAll();
      req3.get(10, TimeUnit.SECONDS).send().receiveAll();
      controller.getRpcManager().stopBlocking();

      //site1 must be online now
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i, null);
         assertSame(SiteState.ONLINE, manager.getSiteState(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(siteName(2)));
      }

      //wait for state transfer to finish
      eventuallyEquals(StateTransferStatus.SEND_OK,
            () -> controller.getStateTransferManager().getStatus().get(remoteSite));

      //check data
      checkDataInSite0And1(method, null);
   }

   public void testNewSiteMasterStartsStateTransfer(Method method) throws Exception {
      String remoteSite = siteName(1); //site1 is the async one

      //we need at least one node with offline status
      //we make all of them to put some data
      takeSiteOffline(null, remoteSite);
      takeSiteOffline(null, siteName(2));
      setAutoStateTransferMode(null, remoteSite, XSiteStateTransferMode.AUTO);

      //lets put some data
      insertDataInSite0(method, null);

      //make sure data didn't go through
      checkNoDataInSite1(method, null);

      SiteMasterController oldSiteMaster = findSiteMaster(null);
      //let the state command go through
      SiteMasterController newSiteMaster = getSiteMasterController(
            oldSiteMaster.managerIndex + 1 % defaultNumberOfNodes(),
            XSiteStatePushCommand.class,
            StateTransferStartCommand.class, StateResponseCommand.class,
            StateTransferCancelCommand.class,
            IracRequestStateCommand.class, IracStateResponseCommand.class,
            IracUpdateVersionCommand.class, IracCleanupKeysCommand.class,
            IracTombstoneStateResponseCommand.class);

      //reset current site master
      oldSiteMaster.getRpcManager().stopBlocking();

      //the JGroups events triggers this command where NodeB checks if it needs to start the transfer
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req1 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);
      // req2 no longer triggered since only site-master trigger the SiteViewChanged event
      // req3 removed, the site status is global and store in an internal cache
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteStateTransferStartSendCommand>> req4 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteStateTransferStartSendCommand.class);

      //block sites up event and wait until received
      newSiteMaster.getStateTransferManager().startBlockSiteUpEvent();

      site(0).kill(0);
      site(0).waitForClusterToForm(null);

      //new view should be installed
      //check if it is the correct event, it creates a new connection so the event contains the 3 sites
      //updated: the new code filters out the local site, so the event only has 2 sites
      newSiteMaster.getStateTransferManager().awaitAndStopBlockingAndAssert(siteName(1), siteName(2)).run();

      //make sure the commands are blocked
      req1.get(10, TimeUnit.SECONDS).send().receiveAll();
      req4.get(10, TimeUnit.SECONDS).send().receiveAll();
      newSiteMaster.getRpcManager().stopBlocking();

      //site1 must be online now
      for (int i = 0; i < defaultNumberOfNodes() - 1; ++i) {
         TakeOfflineManager manager = takeOfflineManager(i, null);
         assertSame(SiteState.ONLINE, manager.getSiteState(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(siteName(2)));
      }

      //wait for state transfer to finish
      eventuallyEquals(StateTransferStatus.SEND_OK,
            () -> newSiteMaster.getStateTransferManager().getStatus().get(remoteSite));

      //check data
      checkDataInSite0And1(method, null);
   }

   public void testInitialStateTransferDuringCacheStart(Method method) throws InterruptedException {
      var remoteSite = siteName(1); //site1 is the async one
      final var cacheName = "initial-state-transfer-1";

      // create cache in site0
      createCache(0, cacheName);

      // take offline to put data to ensure the state transfer happens
      takeSiteOffline(cacheName, remoteSite);
      takeSiteOffline(cacheName, siteName(2));
      setAutoStateTransferMode(cacheName, remoteSite, XSiteStateTransferMode.AUTO);

      // put some data
      insertDataInSite0(method, cacheName);

      // Initial state transfer should ignore the status. Switched to online for asserting that condition.
      bringSiteOnline(cacheName, remoteSite);

      var siteMasterController = findSiteMaster(cacheName);

      // Let's block the event
      siteMasterController.getStateTransferManager().startBlockSiteUpEvent();
      // no need to block since it is the same code
      siteMasterController.getRpcManager().stopBlocking();

      // create the cache
      createCache(1, cacheName);

      // let see if it is empty
      checkNoDataInSite1(method, cacheName);

      // Let's wait for the event and resume it
      siteMasterController.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite).run();

      //wait for state transfer to finish
      eventuallyEquals(StateTransferStatus.SEND_OK,
            () -> siteMasterController.getStateTransferManager().getStatus().get(remoteSite));

      //check data
      checkDataInSite0And1(method, cacheName);
   }

   public void testInitialStateTransferDoesNotStartWithManual(Method method) throws InterruptedException, ExecutionException, TimeoutException {
      var remoteSite = siteName(1); //site1 is the async one
      final var cacheName = "initial-state-transfer-2";

      // create cache in site0
      createCache(0, cacheName);

      // take offline to put data to ensure the state transfer happens
      takeSiteOffline(cacheName, remoteSite);
      takeSiteOffline(cacheName, siteName(2));
      setAutoStateTransferMode(cacheName, remoteSite, XSiteStateTransferMode.AUTO);

      // put some data
      insertDataInSite0(method, cacheName);

      // Initial state transfer should ignore the status. Switched to online for asserting that condition.
      bringSiteOnline(cacheName, remoteSite);

      var siteMasterController = findSiteMaster(cacheName);

      // Let's block the event
      siteMasterController.getStateTransferManager().startBlockSiteUpEvent();

      // only one node set to manual is enough to prevent the state transfer to happen
      stateTransferManager((siteMasterController.managerIndex + 1) % defaultNumberOfSites(), cacheName).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.MANUAL);

      // create the cache
      createCache(1, cacheName);

      // let see if it is empty
      checkNoDataInSite1(method, cacheName);

      var req = siteMasterController.getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);

      // Let's wait for the event and resume it
      siteMasterController.getStateTransferManager().awaitAndStopBlockingAndAssert(remoteSite).run();

      //we expect the coordinator to send a command to check the XSiteStateTransferMode
      req.get(30, TimeUnit.SECONDS).send().receiveAll();
      //check no data
      checkNoDataInSite1(method, cacheName);
   }

   @Override
   protected int defaultNumberOfSites() {
      return 3;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      builder.clustering().hash().numSegments(21);
      // code changed, the receiver site is the one that notifies the sender site
      if (siteIndex == 0) {
         builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      } else if (siteIndex == 1) {
         builder.sites().addBackup().site(siteName(0)).strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      } else {
         builder.sites().addBackup().site(siteName(0)).strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      }
      return builder;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      cleanupTasks.forEach(Runnable::run);
      cleanupTasks.clear();
      while (site(0).cacheManagers().size() < defaultNumberOfNodes()) {
         //noinspection resource
         site(0).addCacheManager(null, defaultGlobalConfigurationForSite(0), defaultConfigurationForSite(0), false);
      }
      site(0).waitForClusterToForm(null);
      super.clearContent();
   }

   private void createCache(int siteIdx, String cacheName) {
      //noinspection resource
      manager(siteIdx, 0)
            .administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache(cacheName, defaultConfigurationForSite(siteIdx).build());
      site(siteIdx).waitForClusterToForm(cacheName);
   }

   private void insertDataInSite0(Method method, String cacheName) {
      for (var i = 0; i < nrKeys; ++i) {
         cache(0, 0, cacheName).put(TestingUtil.k(method, i), TestingUtil.v(method, i));
      }
   }

   private void checkNoDataInSite1(Method method, String cacheName) {
      for (var i = 0; i < nrKeys; ++i) {
         assertNull(cache(1, 0, cacheName).get(TestingUtil.k(method, i)));
      }
   }

   private void checkDataInSite0And1(Method method, String cacheName) {
      for (var i = 0; i < nrKeys; ++i) {
         var key = TestingUtil.k(method, i);
         var value = TestingUtil.v(method, i);
         assertEquals(value, cache(0, 0, cacheName).get(key));
         assertEquals(value, cache(1, 0, cacheName).get(key));
      }
   }

   private void takeSiteOffline(String cacheName, String remoteSite) {
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         var manager = takeOfflineManager(i, cacheName);
         assertNotSame(TakeSiteOfflineResponse.TSOR_NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertEquals(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   private void bringSiteOnline(String cacheName, String remoteSite) {
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         var manager = takeOfflineManager(i, cacheName);
         assertNotSame(BringSiteOnlineResponse.BSOR_NO_SUCH_SITE, manager.bringSiteOnline(remoteSite));
         assertEquals(SiteState.ONLINE, manager.getSiteState(remoteSite));
      }
   }

   private void setAutoStateTransferMode(String cacheName, String remoteSite, XSiteStateTransferMode mode) {
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         var manager = stateTransferManager(i, cacheName);
         manager.setAutomaticStateTransfer(remoteSite, mode);
         assertEquals(mode, manager.stateTransferMode(remoteSite));
      }
   }

   private TakeOfflineManager takeOfflineManager(int managerIndex, String cacheName) {
      return TestingUtil.extractComponent(cache(0, managerIndex, cacheName), TakeOfflineManager.class);
   }

   @SafeVarargs
   private SiteMasterController findSiteMaster(String cacheName, Class<? extends CacheRpcCommand>... excludedCommands) {
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         var manager = manager(0, i);
         var relay2 = findRelay2(manager);
         if (relay2.isPresent() && relay2.get().isSiteMaster()) {
            //we have a single site master, this must be the coordinator as well.
            assertTrue(TestingUtil.extractGlobalComponent(manager, Transport.class).isCoordinator());
            //we need to replace the RpcManager before XSiteStateTransferManager
            //so the XSiteStateTransferManager gets the new RpcManager
            var cache = cacheName == null ? manager.getCache() : manager.getCache(cacheName);
            var rpcManager = ControlledRpcManager.replaceRpcManager(cache, excludedCommands);
            rpcManager.addExcludedCommand(IracCleanupKeysCommand.class);
            var stateTransferManager = ControlledXSiteStateTransferManager.extract(cache);
            cleanupTasks.add(() -> {
               try {
                  rpcManager.revertRpcManager();
                  ControlledXSiteStateTransferManager.revertXsiteStateTransferManager(cache);
               } catch (IllegalLifecycleStateException e) {
                  log.debug("Ignored exception during cleanup", e);
               }
            });
            return new SiteMasterController(relay2.get(), stateTransferManager, rpcManager, i);
         }
      }
      throw new IllegalStateException();
   }

   private static void triggerSiteUpEvent(SiteMasterController controller, String site) {
      controller.getRelay2().getRouteStatusListener().sitesUp(site);
   }

   private static Optional<RELAY2> findRelay2(EmbeddedCacheManager manager) {
      return Optional.ofNullable(TestingUtil.extractJChannel(manager).getProtocolStack().findProtocol(RELAY2.class));
   }

   private XSiteStateTransferManager stateTransferManager(int managerIndex, String cacheName) {
      return TestingUtil.extractComponent(cache(0, managerIndex, cacheName), XSiteStateTransferManager.class);
   }

   @SafeVarargs
   private SiteMasterController getSiteMasterController(int index, Class<? extends CacheRpcCommand>... excludedCommands) {
      EmbeddedCacheManager manager = manager(0, index);
      Optional<RELAY2> relay2 = findRelay2(manager);
      if (relay2.isPresent()) {
         //we need to replace the RpcManager before XSiteStateTransferManager
         //so the XSiteStateTransferManager gets the new RpcManager
         var rpcManager = ControlledRpcManager.replaceRpcManager(manager.getCache(), excludedCommands);
         var stateTransferManager = ControlledXSiteStateTransferManager.extract(manager.getCache());
         cleanupTasks.add(() -> {
            try {
               rpcManager.revertRpcManager();
               ControlledXSiteStateTransferManager.revertXsiteStateTransferManager(manager.getCache());
            } catch (IllegalLifecycleStateException e) {
               log.debug("Ignored exception during cleanup", e);
            }
         });
         return new SiteMasterController(relay2.get(), stateTransferManager, rpcManager, index);
      }
      throw new IllegalStateException();
   }

   private static class SiteMasterController {
      private final RELAY2 relay2;
      private final ControlledXSiteStateTransferManager stateTransferManager;
      private final ControlledRpcManager rpcManager;
      private final int managerIndex;

      private SiteMasterController(RELAY2 relay2, ControlledXSiteStateTransferManager stateTransferManager,
                                   ControlledRpcManager rpcManager, int managerIndex) {
         this.relay2 = relay2;
         this.stateTransferManager = stateTransferManager;
         this.rpcManager = rpcManager;
         this.managerIndex = managerIndex;
      }

      RELAY2 getRelay2() {
         return relay2;
      }

      ControlledXSiteStateTransferManager getStateTransferManager() {
         return stateTransferManager;
      }

      ControlledRpcManager getRpcManager() {
         return rpcManager;
      }
   }
}
