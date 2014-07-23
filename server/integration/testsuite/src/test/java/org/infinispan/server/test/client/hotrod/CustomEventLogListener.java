package org.infinispan.server.test.client.hotrod;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class CustomEventLogListener {
   BlockingQueue<CustomEvent> customEvents = new ArrayBlockingQueue<CustomEvent>(128);

   CustomEvent pollEvent() {
      try {
         CustomEvent event = customEvents.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   public void expectNoEvents() {
      assertEquals(0, customEvents.size());
   }

   public void expectSingleCustomEvent(Integer key, String value) {
      CustomEvent event = pollEvent();
      assertEquals(key, event.key);
      assertEquals(value, event.value);
   }

   @ClientCacheEntryCreated
   @ClientCacheEntryModified
   @ClientCacheEntryRemoved
   @SuppressWarnings("unused")
   public void handleCustomEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
      customEvents.add(e.getEventData());
   }

}
