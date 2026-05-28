package org.infinispan.xsite.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * A {@link XSiteStateTransferManager} implementation that intercepts and controls the {@link
 * XSiteStateTransferManager#startAutomaticStateTransferTo(ByteString, boolean)} method.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
class ControlledXSiteStateTransferManager extends AbstractDelegatingXSiteStateTransferManager {
   private static final Log log = LogFactory.getLog(ControlledXSiteStateTransferManager.class);
   private static final long TIMEOUT_MILLIS = 20000;

   @GuardedBy("this")
   private List<RemoteSiteRequest> sitesUp;

   private ControlledXSiteStateTransferManager(XSiteStateTransferManager delegate) {
      super(delegate);
   }

   public static ControlledXSiteStateTransferManager extract(Cache<?, ?> cache) {
      return AbstractDelegatingXSiteStateTransferManager
            .wrapCache(cache, ControlledXSiteStateTransferManager::new, ControlledXSiteStateTransferManager.class);
   }

   @Override
   public void startAutomaticStateTransferTo(ByteString remoteSite, boolean ignoreStatus) {
      synchronized (this) {
         if (sitesUp != null) {
            log.tracef("Blocking automatic state transfer with sites %s", remoteSite);
            sitesUp.add(new RemoteSiteRequest(remoteSite, ignoreStatus));
            sitesUp.sort(null);
            notifyAll();
            return;
         }
      }
      super.startAutomaticStateTransferTo(remoteSite, ignoreStatus);
   }

   public synchronized void startBlockSiteUpEvent() {
      assertNull(sitesUp);
      sitesUp = new ArrayList<>(2);
   }

   public Runnable awaitAndStopBlockingAndAssert(String remoteSite) throws InterruptedException {
      return awaitAndStopBlockingAndAssert(Collections.singletonList(ByteString.fromString(remoteSite)));
   }

   public Runnable awaitAndStopBlockingAndAssert(String site1, String site2) throws InterruptedException {
      var list = Arrays.asList(ByteString.fromString(site1), ByteString.fromString(site2));
      list.sort(null);
      return awaitAndStopBlockingAndAssert(list);
   }

   private Runnable awaitAndStopBlockingAndAssert(List<ByteString> expectedSites) throws InterruptedException {
      List<RemoteSiteRequest> siteUpCopy;
      synchronized (this) {
         assertNotNull(sitesUp);
         var endTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
         var waitTime = TIMEOUT_MILLIS;
         while (sitesUp.size() < expectedSites.size() && waitTime > 0) {
            this.wait(waitTime);
            waitTime = endTime - System.currentTimeMillis();
         }
         siteUpCopy = List.copyOf(sitesUp);
         // Clear but keep non-null so that any delayed events arriving during .run()
         // are silently captured instead of leaking through to the real impl (which would
         // consume ControlledRpcManager waiters and cause spurious TimeoutExceptions).
         sitesUp.clear();
      }
      assertEquals(expectedSites, siteUpCopy.stream().map(r -> r.remoteSite).collect(Collectors.toList()));
      return () -> {
         siteUpCopy.forEach(RemoteSiteRequest::run);
         synchronized (ControlledXSiteStateTransferManager.this) {
            sitesUp = null;
         }
      };
   }

   private class RemoteSiteRequest implements Comparable<RemoteSiteRequest> {
      final ByteString remoteSite;
      final boolean force;

      private RemoteSiteRequest(ByteString remoteSite, boolean force) {
         this.remoteSite = remoteSite;
         this.force = force;
      }

      void run() {
         ControlledXSiteStateTransferManager.super.startAutomaticStateTransferTo(remoteSite, force);
      }

      @Override
      public int compareTo(RemoteSiteRequest o) {
         return remoteSite.compareTo(o.remoteSite);
      }
   }
}
