package org.infinispan.xsite.statetransfer.failures;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.xsite.XSiteAdminOperations.SUCCESS;
import static org.testng.AssertJUnit.*;

/**
 * Tests the Cross-Site replication state transfer with a broken connection between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "xsite", testName = "xsite.statetransfer.failures.StateTransferLinkFailuresTest")
public class StateTransferLinkFailuresTest extends AbstractTwoSitesTest {

   protected static final int NR_KEYS = 20; //10 * chunk size

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

   public void testStartStateTransferWithoutLink() throws Exception {
      initBeforeTest();
      List<ControllerTransport> transports = replaceTransportInSite(LON);
      for (ControllerTransport transport : transports) {
         transport.fail = true;
      }

      assertTrue(!SUCCESS.equals(extractComponent(cache(LON, 0), XSiteAdminOperations.class).pushState(NYC)));
      assertDataInSite(LON);
      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            AssertJUnit.assertTrue(cache.isEmpty());
         }
      });
      assertTrue(getStatus(LON).isEmpty());

   }

   public void testLinkBrokenDuringStateTransfer() {
      initBeforeTest();
      initBeforeTest();
      List<ControllerTransport> transports = replaceTransportInSite(LON);
      for (ControllerTransport transport : transports) {
         transport.failAfterFirstChunk = true;
      }

      startStateTransfer(cache(LON, 0), NYC);
      assertOnline(LON, NYC);

      assertEventuallyInSite(LON, new EventuallyAssertCondition<Object, Object>() {
         @Override
         public boolean assertInCache(Cache<Object, Object> cache) {
            return extractComponent(cache, XSiteStateTransferManager.class).getRunningStateTransfers().isEmpty();
         }
      }, 1, TimeUnit.MINUTES);

      assertEquals(1, getStatus(LON).size());
      assertEquals(XSiteStateTransferManager.STATUS_ERROR, getStatus(LON).get(NYC));

      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            //link is broken. NYC is still expecting state.
            assertTrue(extractComponent(cache, CommitManager.class).isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
            assertEquals(LON, extractComponent(cache, XSiteAdminOperations.class).getSendingSiteName());
         }
      });

      assertEquals(SUCCESS, extractComponent(cache(NYC, 0), XSiteAdminOperations.class).cancelReceiveState(LON));

      assertInSite(NYC, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            assertFalse(extractComponent(cache, CommitManager.class).isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
            assertNull(extractComponent(cache, XSiteAdminOperations.class).getSendingSiteName());
         }
      });

      assertEquals(SUCCESS, extractComponent(cache(LON, 0), XSiteAdminOperations.class).clearPushStateStatus());
      assertTrue(getStatus(LON).isEmpty());
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

   @Override
   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      builder.stateTransfer().chunkSize(2).timeout(2000);
   }


   private void putData() {
      for (int i = 0; i < NR_KEYS; ++i) {
         cache(LON, 0).put(key(Integer.toString(i)), val(Integer.toString(i)));
      }
   }

   private void initBeforeTest() {
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

   private void startStateTransfer(Cache<?, ?> coordinator, String toSite) {
      XSiteAdminOperations operations = extractComponent(coordinator, XSiteAdminOperations.class);
      assertEquals(SUCCESS, operations.pushState(toSite));
   }

   private void takeSiteOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(SUCCESS, operations.takeSiteOffline(remoteSite));
   }

   private void assertOffline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.OFFLINE, operations.siteStatus(remoteSite));
   }

   private void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(XSiteAdminOperations.ONLINE, operations.siteStatus(remoteSite));
   }

   private Map<String, String> getStatus(String localSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      return operations.getPushStateStatus();
   }

   private void assertDataInSite(String siteName) {
      assertInSite(siteName, new AssertCondition<Object, Object>() {
         @Override
         public void assertInCache(Cache<Object, Object> cache) {
            for (int i = 0; i < NR_KEYS; ++i) {
               assertEquals(val(Integer.toString(i)), cache.get(key(Integer.toString(i))));
            }
         }
      });
   }

   private List<ControllerTransport> replaceTransportInSite(String site) {
      List<ControllerTransport> transports = new ArrayList<>(site(site).cacheManagers().size());
      for (CacheContainer cacheContainer : site(site).cacheManagers()) {
         transports.add(wrapGlobalComponent(cacheContainer,
                                            Transport.class,
                                            new TestingUtil.WrapFactory<Transport, ControllerTransport, CacheContainer>() {
                                               @Override
                                               public ControllerTransport wrap(CacheContainer wrapOn, Transport current) {
                                                  return new ControllerTransport(current);
                                               }
                                            }, true));
      }
      return transports;
   }

   private static class ControllerTransport extends AbstractDelegatingTransport {

      private volatile boolean fail;
      private volatile boolean failAfterFirstChunk;

      public ControllerTransport(Transport actual) {
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
