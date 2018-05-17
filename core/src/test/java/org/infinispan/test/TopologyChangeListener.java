package org.infinispan.test;

import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;

@Listener(observation = Listener.Observation.POST)
public class TopologyChangeListener {
   private final CountDownLatch latch = new CountDownLatch(1);

   public static TopologyChangeListener install(Cache cache) {
      TopologyChangeListener listener = new TopologyChangeListener();
      cache.addListener(listener);
      return listener;
   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent event) {
      latch.countDown();
   }

   public void await() throws InterruptedException {
      await(10, TimeUnit.SECONDS);
   }

   public void await(long time, TimeUnit unit) throws InterruptedException {
      assertTrue(latch.await(time, unit), "View change not seen after " + time + " " + unit);
   }
}
