package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.XSiteResponseImpl;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteProviderDelegator;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.testng.annotations.Test;

/**
 * Cross-Site replication state transfer tests. It tests topology changes in producer site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.failures.SiteProviderTopologyChangeTest")
public class SiteProviderTopologyChangeTest extends AbstractTopologyChangeTest {

   public SiteProviderTopologyChangeTest() {
      super();
   }

   public void testJoinAfterXSiteST() throws Exception {
      doTopologyChangeAfterXSiteStateTransfer(TopologyEvent.JOIN);
   }

   public void testLeaveAfterXSiteST() throws Exception {
      doTopologyChangeAfterXSiteStateTransfer(TopologyEvent.LEAVE);
   }

   public void testCoordinatorLeaveAfterXSiteST() throws Exception {
      doTopologyChangeAfterXSiteStateTransfer(TopologyEvent.COORDINATOR_LEAVE);
   }

   public void testSiteMasterLeaveAfterXSiteST() throws Exception {
      doTopologyChangeAfterXSiteStateTransfer(TopologyEvent.SITE_MASTER_LEAVE);
   }

   public void testJoinDuringXSiteST() throws Exception {
      doTopologyChangeDuringXSiteStateTransfer(TopologyEvent.JOIN);
   }

   public void testLeaveDuringXSiteST() throws Exception {
      doTopologyChangeDuringXSiteStateTransfer(TopologyEvent.LEAVE);
   }

   public void testCoordinatorLeaveDuringXSiteST() throws Exception {
      doTopologyChangeDuringXSiteStateTransfer(TopologyEvent.COORDINATOR_LEAVE);
   }

   public void testSiteMasterLeaveDuringXSiteST() throws Exception {
      doTopologyChangeDuringXSiteStateTransfer(TopologyEvent.SITE_MASTER_LEAVE);
   }

   public void testXSiteSTDuringJoin() throws Exception {
      doXSiteStateTransferDuringTopologyChange(TopologyEvent.JOIN);
   }

   public void testXSiteSTDuringLeave() throws Exception {
      doXSiteStateTransferDuringTopologyChange(TopologyEvent.LEAVE);
   }

   public void testXSiteSTDuringSiteMasterLeave() throws Exception {
      doXSiteStateTransferDuringTopologyChange(TopologyEvent.SITE_MASTER_LEAVE);
   }

   /**
    * the test node starts and finishes the x-site state transfer (some other node didn't sent anything). the topology
    * change is triggered.
    */
   private void doTopologyChangeAfterXSiteStateTransfer(TopologyEvent event) throws Exception {
      log.debugf("Start topology change after x-site state transfer with %s", event);
      initBeforeTest();

      log.debug("Setting blocking conditions");
      final TestCaches<Object, Object> testCaches = createTestCache(event, LON);

      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s", addressOf(testCaches.controllerCache),
            addressOf(testCaches.coordinator),
            testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      ControlledXSiteStateProvider xSiteStateProvider;
      if (testCaches.removeIndex >= 0) {
         log.debugf("Discard x-site state transfer start command in cache %s to remove",
               addressOf(cache(LON, testCaches.removeIndex)));
         xSiteStateProvider = wrapComponent(cache(LON, testCaches.removeIndex), XSiteStateProvider.class,
               DiscardXSiteStateProvider::new);
      } else {
         log.debugf("Block x-site state transfer start command in cache %s", addressOf(cache(LON, 1)));
         xSiteStateProvider = wrapComponent(cache(LON, 1), XSiteStateProvider.class, BlockingXSiteStateProvided::new);
      }

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      //the controller cache will finish the state transfer before the cache topology command
      log.debug("Await until X-Site state transfer is finished!");
      eventually(() -> extractComponent(testCaches.controllerCache, XSiteStateProvider.class).getCurrentStateSending()
                                                                                             .isEmpty(),
            TimeUnit.SECONDS.toMillis(30));

      triggerTopologyChange(LON, testCaches.removeIndex).get();

      awaitLocalStateTransfer(LON);

      log.debug("Let the blocked x-site state transfer request to proceed");
      xSiteStateProvider.stopAndUnblock();

      awaitXSiteStateSent(LON);

      log.debug("Check data in both sites.");
      assertData();
   }

   /**
    * the test node starts the x-site state transfer and sends a chunk of data. the next chunk is blocked and we trigger
    * the cache topology change.
    */
   private void doTopologyChangeDuringXSiteStateTransfer(TopologyEvent event) throws Exception {
      log.debugf("Start topology change during x-site state transfer with %s", event);
      initBeforeTest();

      final TestCaches<Object, Object> testCaches = createTestCache(event, LON);
      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s", addressOf(testCaches.controllerCache),
            addressOf(testCaches.coordinator),
            testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      //the test node will start the x-site state transfer and it will block. next, it the topology will change.
      //strategy: let the first push command to proceed a block the next one.
      BlockXSiteStateRpcManager rpcManager = wrapComponent(testCaches.controllerCache, RpcManager.class,
            BlockXSiteStateRpcManager::new);

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      // wait for first state transfer chunk
      rpcManager.awaitCommand();
      // trigger the topology change and wait until it is installed
      triggerTopologyChange(LON, testCaches.removeIndex).get();
      // let the state transfer chunk go
      rpcManager.continueCommand();

      awaitLocalStateTransfer(LON);
      awaitXSiteStateSent(LON);

      assertData();
   }

   /**
    * x-site state transfer is triggered during a cache topology change.
    */
   private void doXSiteStateTransferDuringTopologyChange(TopologyEvent event) throws Exception {
      log.debugf("Start topology change during x-site state transfer with %s", event);
      initBeforeTest();

      final TestCaches<Object, Object> testCaches = createTestCache(event, LON);
      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s",
            addressOf(testCaches.controllerCache),
            addressOf(testCaches.coordinator),
            testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      BlockingLocalTopologyManager topologyManager =
            replaceTopologyManagerDefaultCache(testCaches.controllerCache.getCacheManager());

      final Future<Void> topologyEventFuture = triggerTopologyChange(LON, testCaches.removeIndex);

      // We could get either the NO_REBALANCE update or the READ_OLD rebalance start first
      BlockingLocalTopologyManager.BlockedTopology blockedTopology = topologyManager.expectTopologyUpdate();

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      blockedTopology.unblock();
      topologyManager.stopBlocking(true);

      topologyEventFuture.get();

      awaitLocalStateTransfer(LON);
      awaitXSiteStateSent(LON);

      assertData();
   }

   private interface ControlledXSiteStateProvider extends XSiteStateProvider {
      void stopAndUnblock();
   }

   private static class StateTransferRequest {
      private final String siteName;
      private final Address requestor;
      private final int minTopologyId;
      private final XSiteStateProvider provider;

      private StateTransferRequest(String siteName, Address requestor, int minTopologyId, XSiteStateProvider provider) {
         this.siteName = siteName;
         this.requestor = requestor;
         this.minTopologyId = minTopologyId;
         this.provider = provider;
      }

      public void execute() {
         provider.startStateTransfer(siteName, requestor, minTopologyId);
      }
   }

   private static class BlockXSiteStateRpcManager extends AbstractDelegatingRpcManager {

      private final CheckPoint checkPoint;
      private final AtomicBoolean firstChunk;

      BlockXSiteStateRpcManager(RpcManager realOne) {
         super(realOne);
         checkPoint = new CheckPoint();
         firstChunk = new AtomicBoolean();
      }

      @Override
      public <O> XSiteResponse<O> invokeXSite(XSiteBackup backup, XSiteReplicateCommand<O> command) {
         if (command instanceof XSiteStatePushCommand) {
            if (firstChunk.compareAndSet(false, true)) {
               try {
                  checkPoint.trigger("command-blocked");
                  checkPoint.awaitStrict("command-proceed", 30, TimeUnit.SECONDS);
               } catch (InterruptedException | TimeoutException e) {
                  XSiteResponseImpl<O> rsp = new XSiteResponseImpl<>(TIME_SERVICE, backup);
                  rsp.completeExceptionally(e);
                  return rsp;
               }
            }
         }
         return super.invokeXSite(backup, command);
      }

      void awaitCommand() throws InterruptedException, TimeoutException {
         checkPoint.awaitStrict("command-blocked", 30, TimeUnit.SECONDS);
      }

      void continueCommand() {
         checkPoint.triggerForever("command-proceed");
      }
   }

   private static class DiscardXSiteStateProvider extends XSiteProviderDelegator implements
         ControlledXSiteStateProvider {

      DiscardXSiteStateProvider(XSiteStateProvider xSiteStateProvider) {
         super(xSiteStateProvider);
      }

      @Override
      public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
         log.debugf("Discard state transfer request to %s from %s", siteName, requestor);
         //no-op, i.e. discard it!
      }

      @Override
      public void stopAndUnblock() {
         //no-op
      }
   }

   private static class BlockingXSiteStateProvided extends XSiteProviderDelegator implements
         ControlledXSiteStateProvider {

      private final BlockingQueue<StateTransferRequest> queue = new LinkedBlockingQueue<>();
      private volatile boolean enabled = true;

      BlockingXSiteStateProvided(XSiteStateProvider xSiteStateProvider) {
         super(xSiteStateProvider);
      }

      @Override
      public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
         if (enabled) {
            log.debugf("Blocking state transfer request to %s from %s", siteName, requestor);
            queue.add(new StateTransferRequest(siteName, requestor, minTopologyId, xSiteStateProvider));
         } else {
            super.startStateTransfer(siteName, requestor, minTopologyId);
         }
      }


      @Override
      public void stopAndUnblock() {
         enabled = false;
         StateTransferRequest req;
         while ((req = queue.poll()) != null) {
            req.execute();
         }
      }
   }

}
