package org.infinispan.xsite.statetransfer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;

import net.jcip.annotations.GuardedBy;

/**
 * A {@link XSiteStateTransferManager} implementation that intercepts and controls the {@link
 * #startAutomaticStateTransferTo(ByteString)} method.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
class ControlledXSiteStateTransferManager extends AbstractDelegatingXSiteStateTransferManager {
   private static final Log log = LogFactory.getLog(ControlledXSiteStateTransferManager.class);
   private static final long TIMEOUT_MILLIS = 20000;

   @GuardedBy("this")
   private List<ByteString> sitesUp;

   private ControlledXSiteStateTransferManager(XSiteStateTransferManager delegate) {
      super(delegate);
   }

   public static ControlledXSiteStateTransferManager extract(Cache<?, ?> cache) {
      return AbstractDelegatingXSiteStateTransferManager
            .wrapCache(cache, ControlledXSiteStateTransferManager::new, ControlledXSiteStateTransferManager.class);
   }

   @Override
   public void startAutomaticStateTransferTo(ByteString remoteSite) {
      synchronized (this) {
         if (sitesUp != null) {
            log.tracef("Blocking automatic state transfer with sites %s", remoteSite);
            sitesUp.add(remoteSite);
            sitesUp.sort(null);
            notifyAll();
            return;
         }
      }
      super.startAutomaticStateTransferTo(remoteSite);
   }

   public synchronized void startBlockSiteUpEvent() {
      AssertJUnit.assertNull(sitesUp);
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
      List<ByteString> siteUpCopy;
      synchronized (this) {
         AssertJUnit.assertNotNull(sitesUp);
         var endTime = System.currentTimeMillis() + TIMEOUT_MILLIS;
         var waitTime = TIMEOUT_MILLIS;
         while (sitesUp.size() < expectedSites.size() && waitTime > 0) {
            this.wait(waitTime);
            waitTime = endTime - System.currentTimeMillis();
         }
         siteUpCopy = List.copyOf(sitesUp);
         sitesUp = null;
      }
      AssertJUnit.assertEquals(expectedSites, siteUpCopy);
      return () -> siteUpCopy.forEach(super::startAutomaticStateTransferTo);
   }
}
