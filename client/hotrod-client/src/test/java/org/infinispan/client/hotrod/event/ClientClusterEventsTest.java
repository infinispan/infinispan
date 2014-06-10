package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventListener.CustomEvent;
import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.event.ConverterFactory;
import org.infinispan.filter.Converter;
import org.infinispan.server.hotrod.event.KeyValueFilterFactory;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterEventsTest")
public class ClientClusterEventsTest extends MultiHotRodServersTest {

   List<TestKeyValueFilterFactory> filters = new ArrayList<TestKeyValueFilterFactory>();
   List<TestConverterFactory> converters = new ArrayList<TestConverterFactory>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(3, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
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
            c3.remove(11);
            expectNoEvents(eventListener);
            c3.remove(22);
            expectOnlyRemovedEvent(22, eventListener, cache(0));
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
            c3.remove(111);
            eventListener.expectSingleCustomEvent(111, null);
            c3.remove(222);
            eventListener.expectSingleCustomEvent(222, null);
         }
      });
   }

   public void testEventReplayAfterFailover() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      HotRodServer server = server(0);
      builder.addServers(server.getHost() + ":" + server.getPort());
      builder.balancingStrategy(FirstServerAvailableBalancer.class);
      RemoteCacheManager newClient = new RemoteCacheManager(builder.build());
      EventLogListener eventListener = new EventLogListener();
      RemoteCache<Integer, String> c = newClient.getCache();
      c.put(0, "zero");
      c.remove(0);
      c.addClientListener(eventListener);
      c.put(1, "one");
      expectOnlyCreatedEvent(1, eventListener, cache(0));
      findServerAndKill(FirstServerAvailableBalancer.serverToKill);
      c.put(2, "two");
      expectFailoverEvent(eventListener);
      expectUnorderedEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
   }

   private void findServerAndKill(InetSocketAddress addr) {
      for (HotRodServer server : servers) {
         if (server.getPort() == addr.getPort()) {
            HotRodClientTestingUtil.killServers(server);
            TestingUtil.killCacheManagers(server.getCacheManager());
            cacheManagers.remove(server.getCacheManager());
            TestingUtil.blockUntilViewsReceived(50000, false, cacheManagers);
         }
      }
   }

   public static class FirstServerAvailableBalancer implements RequestBalancingStrategy {
      static Log log = LogFactory.getLog(FirstServerAvailableBalancer.class);
      static InetSocketAddress serverToKill;
      private Collection<SocketAddress> servers;
      private final RoundRobinBalancingStrategy delegate = new RoundRobinBalancingStrategy();

      @Override
      public void setServers(Collection<SocketAddress> servers) {
         log.info("Set servers: " + servers);
         this.servers = servers;
         delegate.setServers(servers);
         serverToKill = (InetSocketAddress) servers.iterator().next();
      }

      @Override
      public SocketAddress nextServer(Set<SocketAddress> failedServers) {
         if (failedServers != null && !failedServers.isEmpty())
            return delegate.nextServer(failedServers);
         else {
            log.info("Select " + serverToKill + " for load balancing");
            return serverToKill;
         }
      }
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
