package org.infinispan.statetransfer;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Listener implementation that detects whether a merge or
 * a view change occurred.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Listener
public class MergeOrViewChangeListener {

   private static final Log log = LogFactory.getLog(MergeOrViewChangeListener.class);

   // The latch provides the visibility guarantees
   public boolean merged;

   // The latch provides the visibility guarantees
   public boolean viewChanged;

   private final CountDownLatch latch;

   public MergeOrViewChangeListener(CountDownLatch latch) {
      this.latch = latch;
   }

   @Merged
   @SuppressWarnings("unused")
   public void mergedView(MergeEvent me) {
      log.infof("View merged received %s", me);
      merged = true;
      latch.countDown();
   }

   @ViewChanged
   @SuppressWarnings("unused")
   public void viewChanged(ViewChangedEvent e) {
      log.infof("View change received %s", e);
      viewChanged = true;
      latch.countDown();
   }

}
