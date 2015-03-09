package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;


import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterEventsTest")
public class ClientClusterEventsTest extends MultiHotRodServersTest {

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
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory());
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      servers.add(server);
      return server;
   }

   public void testEventForwarding() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            eventListener.expectNoEvents();
            c3.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache(0));
            c3.put(2, "two");
            eventListener.expectOnlyCreatedEvent(2, cache(0));
            c3.put(3, "three");
            eventListener.expectOnlyCreatedEvent(3, cache(0));
            c3.replace(1, "new-one");
            eventListener.expectOnlyModifiedEvent(1, cache(0));
            c3.replace(2, "new-two");
            eventListener.expectOnlyModifiedEvent(2, cache(0));
            c3.replace(3, "new-three");
            eventListener.expectOnlyModifiedEvent(3, cache(0));
            c3.remove(1);
            eventListener.expectOnlyRemovedEvent(1, cache(0));
            c3.remove(2);
            eventListener.expectOnlyRemovedEvent(2, cache(0));
            c3.remove(3);
            eventListener.expectOnlyRemovedEvent(3, cache(0));
         }
      });
   }

   public void testFilteringInCluster() {
      final StaticFilteredEventLogListener<Integer> eventListener = new StaticFilteredEventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            eventListener.expectNoEvents();
            c3.put(1, "one");
            eventListener.expectNoEvents();
            c3.put(2, "two");
            eventListener.expectOnlyCreatedEvent(2, cache(0));
            c3.remove(1);
            eventListener.expectNoEvents();
            c3.remove(2);
            eventListener.expectOnlyRemovedEvent(2, cache(0));
         }
      });
   }

   public void testConversionInCluster() {
      final StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            eventListener.expectNoEvents();
            c3.put(1, "one");
            eventListener.expectOnlyCreatedCustomEvent(new CustomEvent(1, "one"));
            c3.put(2, "two");
            eventListener.expectOnlyCreatedCustomEvent(new CustomEvent(2, "two"));
            c3.remove(1);
            eventListener.expectOnlyRemovedCustomEvent(new CustomEvent(1, null));
            c3.remove(2);
            eventListener.expectOnlyRemovedCustomEvent(new CustomEvent(2, null));
         }
      });
   }

   public void testEventReplayWithAndWithoutStateAfterFailover() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      HotRodServer server = server(0);
      builder.addServers(server.getHost() + ":" + server.getPort());
      builder.balancingStrategy(StickyServerLoadBalancingStrategy.class);
      RemoteCacheManager newClient = new RemoteCacheManager(builder.build());
      try {
         WithStateEventLogListener<Integer> statefulListener = new WithStateEventLogListener<>();
         EventLogListener<Integer> statelessListener = new EventLogListener<>();
         FailoverEventLogListener<Integer> failoverListener = new FailoverEventLogListener<>();
         RemoteCache<Integer, String> c = newClient.getCache();
         c.put(0, "zero");
         c.remove(0);
         c.addClientListener(statelessListener);
         c.addClientListener(statefulListener);
         c.addClientListener(failoverListener);
         c.put(1, "one");
         statefulListener.expectOnlyCreatedEvent(1, cache(0));
         statelessListener.expectOnlyCreatedEvent(1, cache(0));
         failoverListener.expectOnlyCreatedEvent(1, cache(0));
         findServerAndKill(newClient, servers, cacheManagers);
         c.put(2, "two");
         // Failover expectations
         statelessListener.expectNoEvents();
         statefulListener.expectFailoverEvent();
         failoverListener.expectFailoverEvent();
         // State expectations
         statelessListener.expectNoEvents();
         failoverListener.expectNoEvents();
         statefulListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         c.remove(1);
         c.remove(2);
      } finally {
         killRemoteCacheManager(newClient);
      }
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends FailoverEventLogListener<K> {}

}
