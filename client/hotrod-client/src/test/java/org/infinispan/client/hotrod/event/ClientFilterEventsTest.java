package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.server.hotrod.event.KeyValueFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

@Test(groups = "functional", testName = "client.hotrod.event.ClientFilterEventsTest")
public class ClientFilterEventsTest extends SingleHotRodServerTest {

   TestKeyValueFilterFactory filterFactory = new TestKeyValueFilterFactory();

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.keyValueFilterFactory("test-filter-factory", filterFactory);
      return TestHelper.startHotRodServer(cacheManager, builder);
   }

   public void testFilteredEvents() {
      final FilteredEventLogListener eventListener = new FilteredEventLogListener();
      filterFactory.dynamic = false;
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectNoEvents(eventListener);
            cache.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener, cache());
            cache.remove(1);
            expectNoEvents(eventListener);
            cache.remove(2);
            expectOnlyRemovedEvent(2, eventListener, cache());
         }
      });
   }

   public void testParameterBasedFiltering() {
      final FilteredEventLogListener eventListener = new FilteredEventLogListener();
      filterFactory.dynamic = true;
      withClientListener(eventListener, new Object[]{3}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectNoEvents(eventListener);
            cache.put(2, "two");
            expectNoEvents(eventListener);
            cache.put(3, "three");
            expectOnlyCreatedEvent(3, eventListener, cache());
         }
      });
   }

   public void testFilteredEventsReplay() {
      final FilteredEventLogListener eventListener = new FilteredEventLogListener();
      filterFactory.dynamic = false;
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            expectOnlyCreatedEvent(2, eventListener, cache());
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            expectOnlyRemovedEvent(2, eventListener, cache());
         }
      });
      filterFactory.dynamic = true;
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      withClientListener(eventListener, new Object[]{3}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            expectOnlyCreatedEvent(3, eventListener, cache());
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            cache.remove(3);
            expectOnlyRemovedEvent(3, eventListener, cache());
         }
      });
   }

   static class TestKeyValueFilterFactory implements KeyValueFilterFactory {
      boolean dynamic;
      @Override
      public KeyValueFilter<Integer, String> getKeyValueFilter(final Object[] params) {
         return new KeyValueFilter<Integer, String>() {
            @Override
            public boolean accept(Integer key, String value, Metadata metadata) {
               if (!dynamic && key.equals(2)) // static key
                  return true;

               if (dynamic && params[0].equals(key)) // dynamic key
                  return true;

               return false;
            }
         };
      }
   }

   @ClientListener(filterFactoryName = "test-filter-factory")
   static class FilteredEventLogListener extends EventLogListener {}

}
