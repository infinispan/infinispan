package org.infinispan.notifications.cachelistener;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.testng.AssertJUnit.*;

/**
 * This test is to make sure that when more than 1 listener is installed they don't affect each other
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.MultipleListenerConverterTest")
public class MultipleListenerConverterTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   private static class StringConverter implements CacheEventConverter<Object, Object, String> {
      private final String append;

      public StringConverter(String append) {
         this.append = append;
      }

      @Override
      public String convert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         return String.valueOf(newValue) + "-" + append;
      }
   }

   private static class StringFilterConverter extends AbstractCacheEventFilterConverter {
      private final String append;

      public StringFilterConverter(String append) {
         this.append = append;
      }

      @Override
      public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         return String.valueOf(newValue) + "-" + append;
      }
   }

   public void testMultipleListenersWithDifferentConverters() {
      CacheListener listener1 = new CacheListener();
      CacheListener listener2 = new CacheListener();
      CacheListener listener3 = new CacheListener();
      cache.addListener(listener1, null, new StringConverter("listener-1"));
      cache.addListener(listener2, null, new StringConverter("listener-2"));
      cache.addListener(listener3, null, new StringConverter("listener-3"));

      insertKeyValueAndVerifyListenerNotifications(Arrays.asList(listener1, listener2, listener3));
   }

   public void testMultipleListenersWithDifferentFilterConverters() {
      CacheListener listener1 = new CacheListener();
      CacheListener listener2 = new CacheListener();
      CacheListener listener3 = new CacheListener();

      StringFilterConverter filterConverter1 = new StringFilterConverter("listener-1");
      StringFilterConverter filterConverter2 = new StringFilterConverter("listener-2");
      StringFilterConverter filterConverter3 = new StringFilterConverter("listener-3");

      cache.addListener(listener1, filterConverter1, filterConverter1);
      cache.addListener(listener2, filterConverter2, filterConverter2);
      cache.addListener(listener3, filterConverter3, filterConverter3);

      insertKeyValueAndVerifyListenerNotifications(Arrays.asList(listener1, listener2, listener3));
   }

   private void insertKeyValueAndVerifyListenerNotifications(Collection<CacheListener> listeners) {
      cache.put("key", "value");

      int i = 1;
      for (CacheListener listener : listeners) {
         assertEquals("Listener" + i + "failed", 2, listener.getEvents().size());

         Event event = listener.getEvents().get(0);
         assertEquals("Listener" + i + "failed", Event.Type.CACHE_ENTRY_CREATED, event.getType());
         CacheEntryCreatedEvent createdEvent = (CacheEntryCreatedEvent) event;
         assertTrue("Listener" + i + "failed", createdEvent.isPre());
         assertEquals("Listener" + i + "failed", "key", createdEvent.getKey());
         assertEquals("Listener" + i + "failed", "null-listener-" + i, createdEvent.getValue());

         event = listener.getEvents().get(1);
         assertEquals("Listener" + i + "failed", Event.Type.CACHE_ENTRY_CREATED, event.getType());
         createdEvent = (CacheEntryCreatedEvent) event;
         assertFalse("Listener" + i + "failed", createdEvent.isPre());
         assertEquals("Listener" + i + "failed", "key", createdEvent.getKey());
         assertEquals("Listener" + i + "failed", "value-listener-" + i,createdEvent.getValue());

         ++i;
      }
   }
}
