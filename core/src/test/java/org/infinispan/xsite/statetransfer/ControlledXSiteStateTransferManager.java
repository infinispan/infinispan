package org.infinispan.xsite.statetransfer;

import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link XSiteStateTransferManager} implementation that intercepts and controls the {@link
 * #startAutomaticStateTransfer(Collection)} method.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
class ControlledXSiteStateTransferManager extends AbstractDelegatingXSiteStateTransferManager {
   private static final Log log = LogFactory.getLog(ControlledXSiteStateTransferManager.class);

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
         log.tracef("Blocking automatic state transfer with sites %s", sites);
         event.receive(sites, () -> {
            log.tracef("Resuming automatic state transfer with sites %s");
            super.startAutomaticStateTransfer(sites);
         });
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
