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
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedCustomEvent(new CustomEvent(1, null));
            cache.put(1, "newone");
            eventListener.expectOnlyModifiedCustomEvent(new CustomEvent(1, null));
            cache.put(2, "two");
            eventListener.expectOnlyCreatedCustomEvent(new CustomEvent(2, "two"));
            cache.put(2, "dos");
            eventListener.expectOnlyModifiedCustomEvent(new CustomEvent(2, "dos"));
            cache.remove(1);
            eventListener.expectOnlyRemovedCustomEvent(new CustomEvent(1, null));
            cache.remove(2);
            eventListener.expectOnlyRemovedCustomEvent(new CustomEvent(2, null));
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
