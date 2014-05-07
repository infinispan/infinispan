package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventListener.CustomEvent;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Converter;
import org.infinispan.notifications.ConverterFactory;
import org.infinispan.notifications.KeyValueFilter;
import org.infinispan.notifications.KeyValueFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterEventsTest")
public class ClientClusterEventsTest extends MultiHotRodServersTest {

   List<TestKeyValueFilterFactory> filters = new ArrayList<TestKeyValueFilterFactory>();
   List<TestConverterFactory> converters = new ArrayList<TestConverterFactory>();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      createHotRodServers(3, builder);
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      filters.add(new TestKeyValueFilterFactory());
      serverBuilder.keyValueFilterFactory("test-filter-factory", filters.get(0));
      converters.add(new TestConverterFactory());
      serverBuilder.converterFactory("test-converter-factory", converters.get(0));
      HotRodServer server = TestHelper.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }

   public void testEventForwarding() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            expectNoEvents(eventListener);
            c3.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache(0));
            c3.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener, cache(0));
            c3.put(3, "three");
            expectOnlyCreatedEvent(3, eventListener, cache(0));
            c3.replace(1, "newone");
            expectOnlyModifiedEvent(1, eventListener, cache(0));
            c3.replace(2, "newtwo");
            expectOnlyModifiedEvent(2, eventListener, cache(0));
            c3.replace(3, "newthree");
            expectOnlyModifiedEvent(3, eventListener, cache(0));
            c3.remove(1);
            expectOnlyRemovedEvent(1, eventListener, cache(0));
            c3.remove(2);
            expectOnlyRemovedEvent(2, eventListener, cache(0));
            c3.remove(3);
            expectOnlyRemovedEvent(3, eventListener, cache(0));
         }
      });
   }

   public void testFilteringInCluster() {
      final FilteredEventLogListener eventListener = new FilteredEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            expectNoEvents(eventListener);
            c3.put(11, "oneone");
            expectNoEvents(eventListener);
            c3.put(22, "twotwo");
            expectOnlyCreatedEvent(22, eventListener, cache(0));
         }
      });
   }

   public void testConversionInCluster() {
      final CustomEventListener eventListener = new CustomEventListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            eventListener.expectNoEvents();
            c3.put(111, "oneoneone");
            eventListener.expectSingleCustomEvent(111, "oneoneone");
            c3.put(222, "twotwotwo");
            eventListener.expectSingleCustomEvent(222, "twotwotwo");
         }
      });
   }

   static class TestKeyValueFilterFactory implements KeyValueFilterFactory {
      TestKeyValueFilter filter = new TestKeyValueFilter();
      @Override
      public KeyValueFilter<Integer, String> getKeyValueFilter(final Object[] params) {
         filter.params = params;
         return filter;
      }

      static class TestKeyValueFilter implements KeyValueFilter<Integer, String>, Serializable {
         Object[] params;

         @Override
         public boolean accept(Integer key, String value, Metadata metadata) {
            if (key.equals(22)) // static key
               return true;

            return false;
         }
      }
   }

   @ClientListener(filterFactoryName = "test-filter-factory")
   static class FilteredEventLogListener extends EventLogListener {}

   static class TestConverterFactory implements ConverterFactory {
      @Override
      public Converter<Integer, String, CustomEvent> getConverter(final Object[] params) {
         return new TestConverter();
      }

      static class TestConverter implements Converter<Integer, String, CustomEvent>, Serializable {
         @Override
         public CustomEvent convert(Integer key, String value, Metadata metadata) {
            return new CustomEvent(key, value);
         }
      };
   }

}
