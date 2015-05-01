package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterCustomEventLogListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

@Test(groups = "functional", testName = "client.hotrod.event.ClientCustomFilterEventsTest")
public class ClientCustomFilterEventsTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
      server.addCacheEventFilterConverterFactory("filter-converter-factory", new FilterConverterFactory());
      return server;
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testIncorrectFilterFactory() {
      hotrodServer.addCacheEventFilterFactory("xxx", new IncorrectFilterConverterFactory());
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testIncorrectConverterFactory() {
      hotrodServer.addCacheEventConverterFactory("xxx", new IncorrectFilterConverterFactory());
   }

   public void testFilterCustomEvents() {
      final FilterCustomEventLogListener eventListener = new FilterCustomEventLogListener();
      withClientListener(eventListener, new Object[]{1}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.put(1, "one");
            eventListener.expectCreatedEvent(new CustomEvent(1, null, 1));
            cache.put(1, "newone");
            eventListener.expectModifiedEvent(new CustomEvent(1, null, 2));
            cache.put(2, "two");
            eventListener.expectCreatedEvent(new CustomEvent(2, "two", 3));
            cache.put(2, "dos");
            eventListener.expectModifiedEvent(new CustomEvent(2, "dos", 4));
            cache.remove(1);
            eventListener.expectRemovedEvent(new CustomEvent(1, null, 5));
            cache.remove(2);
            eventListener.expectRemovedEvent(new CustomEvent(2, null, 6));
         }
      });
   }

   public static class IncorrectFilterConverterFactory implements CacheEventFilterFactory, CacheEventConverterFactory {
      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(Object[] params) {
         return null;
      }

      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return null;
      }
   }
}
