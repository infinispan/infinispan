package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Listens for view changes.  Note that you do NOT have to register this listener; it does so automatically when
 * constructed.
 */
@Listener
public class ViewChangeListener {
   CacheManager cm;
   final CountDownLatch latch = new CountDownLatch(1);

   public ViewChangeListener(Cache c) {
      this(c.getCacheManager());
   }

   public ViewChangeListener(CacheManager cm) {
      this.cm = cm;
      cm.addListener(this);
   }

   @ViewChanged
   public void onViewChange(ViewChangedEvent e) {
      latch.countDown();
   }

   /**
    * Blocks for a certain amount of time until a view change is received.  Note that this class will start listening
    * for the view change the moment it is constructed.
    *
    * @param time time to wait
    * @param unit time unit
    */
   public void waitForViewChange(long time, TimeUnit unit) throws InterruptedException {
      if (!latch.await(time, unit)) assert false : "View change not seen after " + time + " " + unit;
   }
}
