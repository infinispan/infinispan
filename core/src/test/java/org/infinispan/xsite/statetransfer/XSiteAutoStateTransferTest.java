package org.infinispan.xsite.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.irac.IracCleanupKeyCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteBringOnlineCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.infinispan.xsite.status.TakeSiteOfflineResponse;
import org.jgroups.JChannel;
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

   public void testSyncStrategyDoNotTriggerStateTransfer() throws InterruptedException {
      String remoteSite = siteName(2); //site2 is the sync one

      //make the remote site offline.
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertNotSame(TakeSiteOfflineResponse.NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }

      SiteMasterController controller = findSiteMaster();

      //block sites up event and wait until received
      SiteUpEvent event = controller.getStateTransferManager().blockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      Collection<String> sitesUp = event.waitForEvent();

      //check if it is the correct event
      assertEquals(1, sitesUp.size());
      assertEquals(remoteSite, sitesUp.iterator().next());

      //let the event continue, it will be handled in this thread, which is fine
      event.continueRunnable();

      //coordinator don't even query other nodes state
      controller.getRpcManager().expectNoCommand(10, TimeUnit.SECONDS);

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testManualModeDoNotTriggerStateTransfer() throws InterruptedException {
      String remoteSite = siteName(1); //site1 is the async one

      //make the remote site offline.
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertNotSame(TakeSiteOfflineResponse.NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
         stateTransferManager(i).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.MANUAL);
      }

      SiteMasterController controller = findSiteMaster();

      //block sites up event and wait until received
      SiteUpEvent event = controller.getStateTransferManager().blockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      Collection<String> sitesUp = event.waitForEvent();

      //check if it is the correct event
      assertEquals(1, sitesUp.size());
      assertEquals(remoteSite, sitesUp.iterator().next());

      //let the event continue, it will be handled in this thread, which is fine
      event.continueRunnable();

      //coordinator don't even query other nodes state
      controller.getRpcManager().expectNoCommand(10, TimeUnit.SECONDS);

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testSingleManualModeDoNotTriggerStateTransfer()
         throws InterruptedException, TimeoutException, ExecutionException {
      String remoteSite = siteName(1); //site1 is the async one

      //make the remote site offline.
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertNotSame(TakeSiteOfflineResponse.NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }

      SiteMasterController controller = findSiteMaster();

      boolean manualModeSet = false;
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         if (i == controller.managerIndex) {
            stateTransferManager(i).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.AUTO);
         } else if (!manualModeSet) {
            stateTransferManager(i).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.MANUAL);
            manualModeSet = true;
         }
         //else, does not mather if the 3rd node is MANUAL or AUTO since it shouldn't start any state transfer if at least one node is MANUAL
      }

      //block sites up event and wait until received
      SiteUpEvent event = controller.getStateTransferManager().blockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      Collection<String> sitesUp = event.waitForEvent();

      //check if it is the correct event
      assertEquals(1, sitesUp.size());
      assertEquals(remoteSite, sitesUp.iterator().next());

      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req = controller
            .getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);

      //let the event continue, it will be handled in this thread, which is fine
      event.continueRunnable();

      //we expect the coordinator to send a command
      ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand> cmd = req.get(30, TimeUnit.SECONDS);
      cmd.send().receiveAll();

      //site status must not change
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
      }
   }

   public void testAutoStateTransfer(Method method) throws InterruptedException, TimeoutException, ExecutionException {
      String remoteSite = siteName(1); //site1 is the async one

      //we need at least one node with offline status
      //we make all of them to put some data
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertNotSame(TakeSiteOfflineResponse.NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
         //site2 is disabled as well just to avoid replicating there
         manager.takeSiteOffline(siteName(2));
         assertTrue(stateTransferManager(i).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.AUTO));
      }

      //lets put some data
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         cache(0, 0).put(TestingUtil.k(method, i), TestingUtil.v(method, i));
      }

      //make sure data didn't go through
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         assertNull(cache(1, 0).get(TestingUtil.k(method, i)));
      }

      SiteMasterController controller = findSiteMaster();

      //let the state command go through
      controller.getRpcManager().excludeCommands(XSiteStatePushCommand.class, IracCleanupKeyCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req1 =
            controller.getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteBringOnlineCommand>> req2 =
            controller.getRpcManager().expectCommandAsync(XSiteBringOnlineCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteStateTransferStartSendCommand>> req3 =
            controller.getRpcManager().expectCommandAsync(XSiteStateTransferStartSendCommand.class);

      //block sites up event and wait until received
      SiteUpEvent event = controller.getStateTransferManager().blockSiteUpEvent();
      triggerSiteUpEvent(controller, remoteSite);
      Collection<String> sitesUp = event.waitForEvent();

      //check if it is the correct event
      assertEquals(1, sitesUp.size());
      assertEquals(remoteSite, sitesUp.iterator().next());
      //let the event continue
      event.continueRunnable();

      //make sure the commands are blocked
      req1.get(10, TimeUnit.SECONDS).send().receiveAll();
      req2.get(10, TimeUnit.SECONDS).send().receiveAll();
      req3.get(10, TimeUnit.SECONDS).send().receiveAll();
      controller.getRpcManager().stopBlocking();

      //site1 must be online now
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertSame(SiteState.ONLINE, manager.getSiteState(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(siteName(2)));
      }

      //wait for state transfer to finish
      eventuallyEquals(StateTransferStatus.SEND_OK,
            () -> controller.getStateTransferManager().getStatus().get(remoteSite));

      //check data
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         String key = TestingUtil.k(method, i);
         String value = TestingUtil.v(method, i);
         assertEquals(value, cache(0, 0).get(key));
         assertEquals(value, cache(1, 0).get(key));
      }
   }

   public void testNewSiteMasterStartsStateTransfer(Method method) throws Exception {
      String remoteSite = siteName(1); //site1 is the async one

      //we need at least one node with offline status
      //we make all of them to put some data
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertNotSame(TakeSiteOfflineResponse.NO_SUCH_SITE, manager.takeSiteOffline(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(remoteSite));
         //site2 is disabled as well just to avoid replicating there
         manager.takeSiteOffline(siteName(2));
         assertTrue(stateTransferManager(i).setAutomaticStateTransfer(remoteSite, XSiteStateTransferMode.AUTO));
      }

      //lets put some data
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         cache(0, 0).put(TestingUtil.k(method, i), TestingUtil.v(method, i));
      }

      //make sure data didn't go through
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         assertNull(cache(1, 0).get(TestingUtil.k(method, i)));
      }

      SiteMasterController oldSiteMaster = findSiteMaster();
      SiteMasterController newSiteMaster = getSiteMasterController(
            oldSiteMaster.managerIndex + 1 % defaultNumberOfNodes());

      //reset current site master
      oldSiteMaster.getRpcManager().stopBlocking();

      //let the state command go through
      newSiteMaster.getRpcManager().excludeCommands(XSiteStatePushCommand.class, StateTransferStartCommand.class,
            StateResponseCommand.class, IracRequestStateCommand.class, IracUpdateVersionCommand.class,
            IracCleanupKeyCommand.class, IracStateResponseCommand.class, StateTransferCancelCommand.class);
      //the JGroups events triggers this command where NodeB checks if it needs to starts the transfer
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req1 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);
      //and we have a second command coming from NodeC. NodeB broadcast the RELAY2 events to all nodes
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteAutoTransferStatusCommand>> req2 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteAutoTransferStatusCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteBringOnlineCommand>> req3 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteBringOnlineCommand.class);
      CompletableFuture<ControlledRpcManager.BlockedRequest<XSiteStateTransferStartSendCommand>> req4 = newSiteMaster
            .getRpcManager().expectCommandAsync(XSiteStateTransferStartSendCommand.class);

      //block sites up event and wait until received
      SiteUpEvent event = newSiteMaster.getStateTransferManager().blockSiteUpEvent();

      site(0).kill(0);
      site(0).waitForClusterToForm(null);

      //new view should be installed
      Collection<String> sitesUp = event.waitForEvent();

      //check if it is the correct event, it creates a new connection so the event contains the 3 sites
      assertEquals(3, sitesUp.size());
      assertTrue(sitesUp.contains(siteName(0)));
      assertTrue(sitesUp.contains(siteName(1)));
      assertTrue(sitesUp.contains(siteName(2)));
      //let the event continue
      event.continueRunnable();

      //make sure the commands are blocked
      req1.get(10, TimeUnit.SECONDS).fail(); //we only need req1 or req2 to succeed,it does not matter which one
      req2.get(10, TimeUnit.SECONDS).send().receiveAll();
      req3.get(10, TimeUnit.SECONDS).send().receiveAll();
      req4.get(10, TimeUnit.SECONDS).send().receiveAll();
      newSiteMaster.getRpcManager().stopBlocking();

      //site1 must be online now
      for (int i = 0; i < defaultNumberOfNodes() - 1; ++i) {
         TakeOfflineManager manager = takeOfflineManager(i);
         assertSame(SiteState.ONLINE, manager.getSiteState(remoteSite));
         assertSame(SiteState.OFFLINE, manager.getSiteState(siteName(2)));
      }

      //wait for state transfer to finish
      eventuallyEquals(StateTransferStatus.SEND_OK,
            () -> newSiteMaster.getStateTransferManager().getStatus().get(remoteSite));

      //check data
      for (int i = 0; i < defaultNumberOfNodes() * 5; ++i) {
         String key = TestingUtil.k(method, i);
         String value = TestingUtil.v(method, i);
         assertEquals(value, cache(0, newSiteMaster.managerIndex).get(key));
         assertEquals(value, cache(1, 0).get(key));
      }
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
      if (siteIndex == 0) {
         //for testing purpose, we only need site0 to backup to site1 & site2
         builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.ASYNC)
                .sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      }
      return builder;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      for (EmbeddedCacheManager manager : site(0).cacheManagers()) {
         ControlledXSiteStateTransferManager.revertXsiteStateTransferManager(manager.getCache());
         RpcManager rpcManager = TestingUtil.extractComponent(manager.getCache(), RpcManager.class);
         if (rpcManager instanceof ControlledRpcManager) {
            ((ControlledRpcManager) rpcManager).revertRpcManager();
         }
      }
      while (site(0).cacheManagers().size() < defaultNumberOfNodes()) {
         site(0).addCacheManager(null, defaultGlobalConfigurationForSite(0), defaultConfigurationForSite(0), false);
      }
      site(0).waitForClusterToForm(null);
      super.clearContent();
   }

   private TakeOfflineManager takeOfflineManager(int managerIndex) {
      return TestingUtil.extractComponent(cache(0, managerIndex), TakeOfflineManager.class);
   }

   private SiteMasterController findSiteMaster() {
      for (int i = 0; i < defaultNumberOfNodes(); ++i) {
         EmbeddedCacheManager manager = manager(0, i);
         Optional<RELAY2> relay2 = findRelay2(manager);
         if (relay2.isPresent() && relay2.get().isSiteMaster()) {
            //we have a single site master, this must be the coordinator as well.
            assertTrue(TestingUtil.extractGlobalComponent(manager, Transport.class).isCoordinator());
            //we need to replace the RpcManager before XSiteStateTransferManager
            //so the XSiteStateTransferManager gets the new RpcManager
            ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(manager.getCache());
            ControlledXSiteStateTransferManager stateTransferManager = ControlledXSiteStateTransferManager
                  .extract(manager.getCache());
            return new SiteMasterController(relay2.get(), stateTransferManager, rpcManager, i);
         }
      }
      throw new IllegalStateException();
   }

   private void triggerSiteUpEvent(SiteMasterController controller, String site) {
      controller.getRelay2().getRouteStatusListener().sitesUp(site);
   }

   private Optional<RELAY2> findRelay2(EmbeddedCacheManager manager) {
      JChannel channel = TestingUtil.extractJChannel(manager);
      RELAY2 relay2 = channel.getProtocolStack().findProtocol(RELAY2.class);
      return relay2 == null ? Optional.empty() : Optional.of(relay2);
   }

   private XSiteStateTransferManager stateTransferManager(int managerIndex) {
      return TestingUtil.extractComponent(cache(0, managerIndex), XSiteStateTransferManager.class);
   }

   private SiteMasterController getSiteMasterController(int index) {
      EmbeddedCacheManager manager = manager(0, index);
      Optional<RELAY2> relay2 = findRelay2(manager);
      if (relay2.isPresent()) {
         //we need to replace the RpcManager before XSiteStateTransferManager
         //so the XSiteStateTransferManager gets the new RpcManager
         ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(manager.getCache());
         ControlledXSiteStateTransferManager stateTransferManager = ControlledXSiteStateTransferManager
               .extract(manager.getCache());
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

      public RELAY2 getRelay2() {
         return relay2;
      }

      public ControlledXSiteStateTransferManager getStateTransferManager() {
         return stateTransferManager;
      }

      public ControlledRpcManager getRpcManager() {
         return rpcManager;
      }
   }
}
