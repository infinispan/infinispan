package org.infinispan.xsite.statetransfer;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.xsite.XSiteAdminOperations.OFFLINE;
import static org.infinispan.xsite.XSiteAdminOperations.ONLINE;
import static org.infinispan.xsite.XSiteAdminOperations.SUCCESS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * Base class for state transfer tests with some utility method.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class AbstractStateTransferTest extends AbstractTwoSitesTest {

   void assertNoStateTransferInReceivingSite(String cacheName) {
      assertInSite(NYC, cacheName, this::assertNotReceivingStateForCache);
   }

   void assertNoStateTransferInSendingSite() {
      assertInSite(LON, cache -> assertTrue(isNotSendingStateForCache(cache)));
   }

   void assertEventuallyNoStateTransferInSendingSite() {
      assertEventuallyInSite(LON, this::isNotSendingStateForCache, 30, TimeUnit.SECONDS);
   }

   void assertEventuallyStateTransferNotRunning() {
      eventually(() -> adminOperations().getRunningStateTransfer().isEmpty(), 30,
            TimeUnit.SECONDS);
   }

   int chunkSize() {
      return cache(LON, 0).getCacheConfiguration().sites().allBackups().get(0).stateTransfer().chunkSize();
   }

   protected void assertEventuallyNoStateTransferInReceivingSite(String cacheName) {
      assertEventuallyInSite(NYC, cacheName, this::isNotReceivingStateForCache, 30, TimeUnit.SECONDS);
   }

   protected void startStateTransfer() {
      startStateTransfer(cache(LON, 0), NYC);
   }

   protected void startStateTransfer(Cache<?, ?> cache, String toSite) {
      XSiteAdminOperations operations = extractComponent(cache, XSiteAdminOperations.class);
      assertEquals(SUCCESS, operations.pushState(toSite));
   }

   protected void takeSiteOffline() {
      XSiteAdminOperations operations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      assertEquals(SUCCESS, operations.takeSiteOffline(NYC));
   }

   protected void assertOffline() {
      XSiteAdminOperations operations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      assertEquals(OFFLINE, operations.siteStatus(NYC));
   }

   protected void assertOnline(String localSite, String remoteSite) {
      XSiteAdminOperations operations = extractComponent(cache(localSite, 0), XSiteAdminOperations.class);
      assertEquals(ONLINE, operations.siteStatus(remoteSite));
   }

   protected XSiteAdminOperations adminOperations() {
      return extractComponent(cache(LON, 0), XSiteAdminOperations.class);
   }

   private void assertNotReceivingStateForCache(Cache<?, ?> cache) {
      CommitManager commitManager = extractComponent(cache, CommitManager.class);
      assertFalse(commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER));
      assertFalse(commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER));
      assertTrue(commitManager.isEmpty());
   }

   private boolean isNotReceivingStateForCache(Cache<?, ?> cache) {
      CommitManager commitManager = extractComponent(cache, CommitManager.class);
      return !commitManager.isTracking(Flag.PUT_FOR_STATE_TRANSFER) &&
            !commitManager.isTracking(Flag.PUT_FOR_X_SITE_STATE_TRANSFER) &&
            commitManager.isEmpty();
   }

   private boolean isNotSendingStateForCache(Cache<?, ?> cache) {
      return extractComponent(cache, XSiteStateProvider.class).getCurrentStateSending().isEmpty();
   }

}
