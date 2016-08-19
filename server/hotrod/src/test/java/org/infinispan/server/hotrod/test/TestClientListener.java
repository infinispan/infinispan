package org.infinispan.server.hotrod.test;

import org.infinispan.notifications.cachelistener.event.Event;

/**
 * @author Galder Zamarreño
 */
public abstract class TestClientListener {

   public void onCreated(TestKeyWithVersionEvent event) {
   } // no-op

   public void onModified(TestKeyWithVersionEvent event) {
   } // no-op

   public void onRemoved(TestKeyEvent event) {
   } // no-op

   public void onCustom(TestCustomEvent event) {
   } // no-op

   public int queueSize(Event.Type eventType) {
      return 0;
   }

   public Object pollEvent(Event.Type eventType) {
      return null;
   }

   public int customQueueSize() {
      return 0;
   }

   public TestCustomEvent pollCustom() {
      return null;
   }

   public abstract byte[] getId();
}
