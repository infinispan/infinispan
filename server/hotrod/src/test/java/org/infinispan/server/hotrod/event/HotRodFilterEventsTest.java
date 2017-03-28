package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodFilterEventsTest")
public class HotRodFilterEventsTest extends HotRodSingleNodeTest {

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServer server = startHotRodServer(cacheManager);
      server.addCacheEventFilterFactory("static-filter-factory", new StaticKeyValueFilterFactory(new byte[]{1, 2, 3}));
      server.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicKeyValueFilterFactory());
      return server;
   }

   public void testFilteredEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] acceptedKey = new byte[]{1, 2, 3};
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("static-filter-factory", Collections.emptyList())), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().remove(key);
         eventListener.expectNoEvents(Optional.empty());
         client().put(key, 0, 0, v(m));
         eventListener.expectNoEvents(Optional.empty());
         client().put(acceptedKey, 0, 0, v(m));
         eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_CREATED);
         client().put(acceptedKey, 0, 0, v(m, "v2-"));
         eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED);
         client().remove(key);
         eventListener.expectNoEvents(Optional.empty());
         client().remove(acceptedKey);
         eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_REMOVED);
      });
   }

   public void testParameterBasedFiltering(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] acceptedKey = new byte[]{4, 5, 6};
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("dynamic-filter-factory", Collections.singletonList(new byte[]{4, 5, 6}))),
                         Optional.empty(), () -> {
               eventListener.expectNoEvents(Optional.empty());
               byte[] key = k(m);
               client().put(key, 0, 0, v(m));
               eventListener.expectNoEvents(Optional.empty());
               client().put(acceptedKey, 0, 0, v(m));
               eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_CREATED);
               client().put(acceptedKey, 0, 0, v(m, "v2-"));
               eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED);
               client().remove(key);
               eventListener.expectNoEvents(Optional.empty());
               client().remove(acceptedKey);
               eventListener.expectSingleEvent(cache, acceptedKey, Event.Type.CACHE_ENTRY_REMOVED);
            });
   }

   public void testFilteredEventsReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] staticAcceptedKey = new byte[]{1, 2, 3};
      byte[] dynamicAcceptedKey = new byte[]{7, 8, 9};
      byte[] key = k(m);
      client().put(key, 0, 0, v(m));
      client().put(staticAcceptedKey, 0, 0, v(m));
      client().put(dynamicAcceptedKey, 0, 0, v(m));
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("static-filter-factory", Collections.emptyList())), Optional.empty(), true, true, () -> {
         eventListener.expectSingleEvent(cache, staticAcceptedKey, Event.Type.CACHE_ENTRY_CREATED);
      });
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("dynamic-filter-factory", Collections.singletonList(new byte[]{7, 8, 9}))),
                         Optional.empty(), true, true, () -> {
               eventListener.expectSingleEvent(cache, dynamicAcceptedKey, Event.Type.CACHE_ENTRY_CREATED);
            });
   }

   public void testFilteredEventsNoReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] staticAcceptedKey = new byte[]{1, 2, 3};
      byte[] dynamicAcceptedKey = new byte[]{7, 8, 9};
      byte[] key = k(m);
      client().put(key, 0, 0, v(m));
      client().put(staticAcceptedKey, 0, 0, v(m));
      client().put(dynamicAcceptedKey, 0, 0, v(m));
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("static-filter-factory", Collections.emptyList())), Optional.empty(), false, true,
                         () -> {
                            eventListener.expectNoEvents(Optional.empty());
                         });
      withClientListener(client(), eventListener, Optional.of(
            new KeyValuePair<>("dynamic-filter-factory", Collections.singletonList(new byte[]{7, 8, 9}))),
                         Optional.empty(), false, true, () -> {
               eventListener.expectNoEvents(Optional.empty());
            });
   }

   static class StaticKeyValueFilterFactory implements CacheEventFilterFactory {
      private byte[] staticKey;

      StaticKeyValueFilterFactory(byte[] staticKey) {
         this.staticKey = staticKey;
      }

      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(Object[] params) {
         return (key, prevValue, prevMetadata, value, metadata, eventType) -> Arrays.equals(((byte[]) key), staticKey);
      }
   }

   static class DynamicKeyValueFilterFactory implements CacheEventFilterFactory {
      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(Object[] params) {
         return (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> {
            byte[] acceptedKey = (byte[]) params[0];
            return Arrays.equals(((byte[]) key), acceptedKey);
         };
      }
   }
}
