package org.infinispan.xsite.statetransfer;

import java.util.Collection;

import org.infinispan.Cache;

/**
 * A {@link XSiteStateTransferManager} implementation that intercepts and controls the {@link
 * #startAutomaticStateTransfer(Collection)} method.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
class ControlledXSiteStateTransferManager extends AbstractDelegatingXSiteStateTransferManager {

   private SiteUpEvent event;

   private ControlledXSiteStateTransferManager(XSiteStateTransferManager delegate) {
      super(delegate);
   }

   public static ControlledXSiteStateTransferManager extract(Cache<?, ?> cache) {
      return AbstractDelegatingXSiteStateTransferManager
            .wrapCache(cache, ControlledXSiteStateTransferManager::new, ControlledXSiteStateTransferManager.class);
   }

   @Override
   public void startAutomaticStateTransfer(Collection<String> sites) {
      SiteUpEvent event = getEvent();
      if (event != null) {
         event.receive(sites, () -> super.startAutomaticStateTransfer(sites));
      } else {
         super.startAutomaticStateTransfer(sites);
      }
   }

   public synchronized SiteUpEvent blockSiteUpEvent() {
      assert event == null;
      event = new SiteUpEvent();
      return event;
   }

   private synchronized SiteUpEvent getEvent() {
      SiteUpEvent event = this.event;
      this.event = null;
      return event;
   }
}
