package org.infinispan.xsite.statetransfer.failures;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LookupMode;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverDelegator;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryDelegator;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.test.TestingUtil.WrapFactory;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManager;

/**
 * Cross-Site replication state transfer tests. It tests topology changes in consumer site.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.failures.SiteConsumerTopologyChangeTest")
public class SiteConsumerTopologyChangeTest extends AbstractTopologyChangeTest {

   public SiteConsumerTopologyChangeTest() {
      super();
   }

   public void testJoinDuringXSiteST() throws InterruptedException, ExecutionException, TimeoutException {
      doTopologyChangeDuringXSiteST(TopologyEvent.JOIN);
   }

   public void testLeaveDuringXSiteST() throws InterruptedException, ExecutionException, TimeoutException {
      doTopologyChangeDuringXSiteST(TopologyEvent.LEAVE);
   }

   public void testSiteMasterLeaveDuringXSiteST() throws InterruptedException, ExecutionException, TimeoutException {
      doTopologyChangeDuringXSiteST(TopologyEvent.SITE_MASTER_LEAVE);
   }

   public void testXSiteDuringJoin() throws InterruptedException, ExecutionException, TimeoutException {
      doXSiteStateTransferDuringTopologyChange(TopologyEvent.JOIN);
   }

   public void testXSiteSTDuringLeave() throws InterruptedException, ExecutionException, TimeoutException {
      doXSiteStateTransferDuringTopologyChange(TopologyEvent.LEAVE);
   }

   /**
    * Site consumer receives some chunks and then, the topology changes.
    */
   private void doTopologyChangeDuringXSiteST(TopologyEvent event) throws TimeoutException, InterruptedException, ExecutionException {
      log.debugf("Start topology change during x-site state transfer with %s", event);
      initBeforeTest();

      final TestCaches<Object, Object> testCaches = createTestCache(event, NYC);
      printTestCaches(testCaches);

      final CheckPoint checkPoint = new CheckPoint();
      final AtomicBoolean discard = new AtomicBoolean(true);

      wrapGlobalComponent(cache(NYC, 0).getCacheManager(),
                          BackupReceiverRepository.class,
                          new WrapFactory<BackupReceiverRepository, BackupReceiverRepository, CacheContainer>() {
                             @Override
                             public BackupReceiverRepository wrap(final CacheContainer wrapOn, final BackupReceiverRepository current) {
                                return new BackupReceiverRepositoryDelegator(current) {

                                   private final Set<Address> addressSet = new HashSet<>();

                                   @Override
                                   public BackupReceiver getBackupReceiver(String originSiteName, String cacheName) {
                                      return new BackupReceiverDelegator(super.getBackupReceiver(originSiteName, cacheName)) {
                                         @Override
                                         public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
                                            log.debugf("Applying state: %s", cmd);
                                            if (!discard.get()) {
                                               delegate.handleStateTransferState(cmd);
                                               return;
                                            }
                                            DistributionManager manager = delegate.getCache().getAdvancedCache().getDistributionManager();
                                            synchronized (addressSet) {
                                               //discard the state message when all member has received at least one chunk!
                                               if (addressSet.size() == 3) {
                                                  checkPoint.trigger("before-block");
                                                  checkPoint.awaitStrict("blocked", 30, TimeUnit.SECONDS);
                                                  delegate.handleStateTransferState(cmd);
                                                  return;
                                               }
                                               for (XSiteState state : cmd.getChunk()) {
                                                  addressSet.add(manager.getPrimaryLocation(state.key(), LookupMode.WRITE));
                                               }
                                            }
                                            delegate.handleStateTransferState(cmd);
                                         }
                                      };
                                   }
                                };
                             }
                          }, true);

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      checkPoint.awaitStrict("before-block", 30, TimeUnit.SECONDS);

      Future<?> topologyChangeFuture = triggerTopologyChange(NYC, testCaches.removeIndex);
      discard.set(false);
      checkPoint.triggerForever("blocked");
      topologyChangeFuture.get();

      awaitXSiteStateSent(LON);
      awaitLocalStateTransfer(NYC);
      awaitXSiteStateReceived(NYC);

      assertData();
   }

   private void doXSiteStateTransferDuringTopologyChange(TopologyEvent event) throws TimeoutException, InterruptedException, ExecutionException {
      /*
      note: this is not tested with SITE_MASTER_LEAVE because it can start the state transfer in NYC.
      (startStateTransfer() throws an exception)
       */
      log.debugf("Start topology change during x-site state transfer with %s", event);
      initBeforeTest();

      final TestCaches<Object, Object> testCaches = createTestCache(event, NYC);
      printTestCaches(testCaches);

      final BlockingLocalTopologyManager topologyManager = replaceTopologyManager(testCaches.controllerCache.getCacheManager());
      final CheckPoint checkPoint = new CheckPoint();

      wrapGlobalComponent(cache(NYC, 0).getCacheManager(),
                          BackupReceiverRepository.class,
                          new WrapFactory<BackupReceiverRepository, BackupReceiverRepository, CacheContainer>() {
                             @Override
                             public BackupReceiverRepository wrap(final CacheContainer wrapOn, final BackupReceiverRepository current) {
                                return new BackupReceiverRepositoryDelegator(current) {
                                   @Override
                                   public BackupReceiver getBackupReceiver(String originSiteName, String cacheName) {
                                      return new BackupReceiverDelegator(super.getBackupReceiver(originSiteName, cacheName)) {
                                         @Override
                                         public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
                                            checkPoint.trigger("before-chunk");
                                            delegate.handleStateTransferState(cmd);
                                         }
                                      };
                                   }
                                };
                             }
                          }, true);

      topologyManager.startBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);

      final Future<Void> topologyEventFuture = triggerTopologyChange(NYC, testCaches.removeIndex);

      topologyManager.waitToBlock(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);

      log.debug("Start x-site state transfer");
      startStateTransfer(testCaches.coordinator, NYC);
      assertOnline(LON, NYC);

      //in the current implementation, the x-site state transfer is not triggered while the rebalance is in progress.
      checkPoint.awaitStrict("before-chunk", 30, TimeUnit.SECONDS);
      topologyManager.stopBlockingAll();

      topologyEventFuture.get();

      awaitXSiteStateSent(LON);
      awaitLocalStateTransfer(NYC);
      awaitXSiteStateReceived(NYC);

      assertData();
   }

}
