package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.test.TestingUtil.WrapFactory;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.XSiteResponseImpl;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRequest;
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
//unstable: it looks like not all cases are handled properly. Needs to be revisited! ISPN-6228
@Test(groups = {"xsite", "unstable"}, testName = "xsite.statetransfer.failures.SiteProviderTopologyChangeTest")
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

   @Test(groups = {"xsite", "unstable"}, description = "See ISPN-6749")
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
      final AtomicReference<StateTransferRequest> pendingRequest = new AtomicReference<>(null);

      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s", addressOf(testCaches.controllerCache),
                 addressOf(testCaches.coordinator), testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      if (testCaches.removeIndex >= 0) {
         log.debugf("Discard x-site state transfer start command in cache %s to remove", addressOf(cache(LON, testCaches.removeIndex)));
         wrapComponent(cache(LON, testCaches.removeIndex), XSiteStateProvider.class,
                       (WrapFactory<XSiteStateProvider, XSiteStateProvider, Cache<?, ?>>) (wrapOn, current) -> new XSiteProviderDelegator(current) {
                          @Override
                          public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
                             log.debugf("Discard state transfer request to %s from %s", siteName, requestor);
                             //no-op, i.e. discard it!
                          }
                       }, true);
      } else {
         log.debugf("Block x-site state transfer start command in cache %s", addressOf(cache(LON, 1)));
         wrapComponent(cache(LON, 1), XSiteStateProvider.class,
                       (WrapFactory<XSiteStateProvider, XSiteStateProvider, Cache<?, ?>>) (wrapOn, current) -> new XSiteProviderDelegator(current) {
                          @Override
                          public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
                             log.debugf("Blocking state transfer request to %s from %s", siteName, requestor);
                             pendingRequest.set(new StateTransferRequest(siteName, requestor, minTopologyId, xSiteStateProvider));
                          }
                       }, true);
      }

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      //the controller cache will finish the state transfer before the cache topology command
      log.debug("Await until X-Site state transfer is finished!");
      eventually(() -> extractComponent(testCaches.controllerCache, XSiteStateProvider.class).getCurrentStateSending().isEmpty(),
                 TimeUnit.SECONDS.toMillis(30));

      Future<Void> topologyEventFuture = triggerTopologyChange(LON, testCaches.removeIndex);

      topologyEventFuture.get();

      awaitLocalStateTransfer(LON);

      if (pendingRequest.get() != null) {
         log.debug("Let the blocked x-site state transfer request to proceed");
         pendingRequest.get().execute();
      }

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
                 addressOf(testCaches.coordinator), testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      //the test node will start the x-site state transfer and it will block. next, it the topology will change.
      //strategy: let the first push command to proceed a block the next one.
      final CheckPoint checkPoint = new CheckPoint();
      final AtomicBoolean firstChunk = new AtomicBoolean(false);
      wrapGlobalComponent(testCaches.controllerCache.getCacheManager(),
                          Transport.class,
                          new WrapFactory<Transport, Transport, CacheContainer>() {
                             @Override
                             public Transport wrap(CacheContainer wrapOn, Transport current) {
                                return new AbstractDelegatingTransport(current) {
                                   @Override
                                   public void start() {
                                      //no-op; avoid re-start the transport again...
                                   }

                                   @Override
                                   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand) {
                                      if (rpcCommand instanceof XSiteStatePushCommand) {
                                         if (firstChunk.compareAndSet(false, true)) {
                                            checkPoint.trigger("before-second-chunk");
                                            try {
                                               checkPoint.awaitStrict("second-chunk", 30, TimeUnit.SECONDS);
                                            } catch (InterruptedException | TimeoutException e) {
                                               var rsp = new XSiteResponseImpl<O>(TIME_SERVICE, backup);
                                               rsp.completeExceptionally(e);
                                               return rsp;
                                            }
                                         }
                                      }
                                      return super.backupRemotely(backup, rpcCommand);
                                   }
                                };
                             }
                          }, true);

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      checkPoint.awaitStrict("before-second-chunk", 30, TimeUnit.SECONDS);

      final Future<Void> topologyEventFuture = triggerTopologyChange(LON, testCaches.removeIndex);

      topologyEventFuture.get();
      checkPoint.triggerForever("second-chunk");

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
      log.debugf("Controlled cache=%s, Coordinator cache=%s, Cache to remove=%s", addressOf(testCaches.controllerCache),
                 addressOf(testCaches.coordinator), testCaches.removeIndex < 0 ? "NONE" : addressOf(cache(LON, testCaches.removeIndex)));

      BlockingLocalTopologyManager topologyManager =
         replaceTopologyManagerDefaultCache(testCaches.controllerCache.getCacheManager());

      final Future<Void> topologyEventFuture = triggerTopologyChange(LON, testCaches.removeIndex);

      // We could get either the NO_REBALANCE update or the READ_OLD rebalance start first
      BlockingLocalTopologyManager.BlockedTopology blockedTopology = topologyManager.expectTopologyUpdate();

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      blockedTopology.unblock();

      topologyEventFuture.get();

      awaitLocalStateTransfer(LON);
      awaitXSiteStateSent(LON);

      assertData();
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

}
