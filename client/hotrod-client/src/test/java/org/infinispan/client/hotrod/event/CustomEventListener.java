package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

@ClientListener(converterFactoryName = "test-converter-factory")
public class CustomEventListener {
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

   public static class CustomEvent implements Serializable {
      final Integer key;
      final String value;
      CustomEvent(Integer key, String value) {
         this.key = key;
         this.value = value;
      }
   }

}
