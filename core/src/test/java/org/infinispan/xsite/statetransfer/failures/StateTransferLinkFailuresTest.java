package org.infinispan.xsite.statetransfer.failures;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.xsite.XSiteAdminOperations.SUCCESS;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferManager.STATUS_ERROR;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests the Cross-Site replication state transfer with a broken connection between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.failures.StateTransferLinkFailuresTest")
public class StateTransferLinkFailuresTest extends AbstractTopologyChangeTest {

   public StateTransferLinkFailuresTest() {
      super();
      this.cleanup = CleanupPhase.AFTER_METHOD;
      this.implicitBackupCache = true;
   }

   /*
   tests:
   * request the state transfer without link to other site.
   * lose link while transferring data
    */

   private static ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   public void testStartStateTransferWithoutLink() {
      initBeforeTest();
      List<ControllerTransport> transports = replaceTransportInSite();
      for (ControllerTransport transport : transports) {
         transport.fail = true;
      }

      assertTrue(!SUCCESS.equals(extractComponent(cache(LON, 0), XSiteAdminOperations.class).pushState(NYC)));
      assertDataInSite(LON);
      assertInSite(NYC, cache -> AssertJUnit.assertTrue(cache.isEmpty()));
      assertTrue(getStatus().isEmpty());

   }

   public void testLinkBrokenDuringStateTransfer() {
      initBeforeTest();
      List<ControllerTransport> transports = replaceTransportInSite();
      for (ControllerTransport transport : transports) {
         transport.failAfterFirstChunk = true;
      }

      startStateTransfer();
      assertOnline(LON, NYC);

      assertEventuallyInSite(LON,
            cache -> extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty(), 1,
            TimeUnit.MINUTES);

      assertEquals(1, getStatus().size());
      assertEquals(STATUS_ERROR, getStatus().get(NYC));

      assertInSite(NYC, cache -> {
         //link is broken. NYC is still expecting state.
         assertTrue(extractComponent(cache, CommitManager.class).isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
         assertEquals(LON, extractComponent(cache, XSiteAdminOperations.class).getSendingSiteName());
      });

      assertEquals(SUCCESS, extractComponent(cache(NYC, 0), XSiteAdminOperations.class).cancelReceiveState(LON));

      assertInSite(NYC, cache -> {
         assertFalse(extractComponent(cache, CommitManager.class).isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
         assertNull(extractComponent(cache, XSiteAdminOperations.class).getSendingSiteName());
      });

      assertEquals(SUCCESS, extractComponent(cache(LON, 0), XSiteAdminOperations.class).clearPushStateStatus());
      assertTrue(getStatus().isEmpty());
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return createConfiguration();
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return createConfiguration();
   }

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      builder.stateTransfer().chunkSize(2).timeout(2000);
   }

   private Map<String, String> getStatus() {
      return adminOperations().getPushStateStatus();
   }

   private List<ControllerTransport> replaceTransportInSite() {
      List<ControllerTransport> transports = new ArrayList<>(site(LON).cacheManagers().size());
      for (CacheContainer cacheContainer : site(LON).cacheManagers()) {
         transports.add(wrapGlobalComponent(cacheContainer,
                                            Transport.class,
               (wrapOn, current) -> new ControllerTransport(current), true));
      }
      return transports;
   }

   private static class ControllerTransport extends AbstractDelegatingTransport {

      private volatile boolean fail;
      private volatile boolean failAfterFirstChunk;

      ControllerTransport(Transport actual) {
         super(actual);
      }

      @Override
      public void start() {
         //no-op; avoid re-start the transport again...
      }

      @Override
      public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
         if (fail) {
            throw new TimeoutException("induced timeout!");
         } else if (failAfterFirstChunk && rpcCommand instanceof XSiteStatePushCommand) {
            fail = true;
         }
         return super.backupRemotely(backups, rpcCommand);
      }
   }
}
