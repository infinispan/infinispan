package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
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
      HotRodServer server = TestHelper.startHotRodServer(cm, serverBuilder);
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
            eventListener.expectOnlyCreatedCustomEvent(1, "one");
            c3.put(2, "two");
            eventListener.expectOnlyCreatedCustomEvent(2, "two");
            c3.remove(1);
            eventListener.expectOnlyRemovedCustomEvent(1, null);
            c3.remove(2);
            eventListener.expectOnlyRemovedCustomEvent(2, null);
         }
      });
   }

   public void testEventReplayWithAndWithoutStateAfterFailover() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      HotRodServer server = server(0);
      builder.addServers(server.getHost() + ":" + server.getPort());
      builder.balancingStrategy(FirstServerAvailableBalancer.class);
      RemoteCacheManager newClient = new RemoteCacheManager(builder.build());
      try {
         WithStateEventLogListener<Integer> withStateEventLogListener = new WithStateEventLogListener<>();
         EventLogListener<Integer> withoutStateEventLogListener = new EventLogListener<>();
         RemoteCache<Integer, String> c = newClient.getCache();
         c.put(0, "zero");
         c.remove(0);
         c.addClientListener(withoutStateEventLogListener);
         c.addClientListener(withStateEventLogListener);
         c.put(1, "one");
         withStateEventLogListener.expectOnlyCreatedEvent(1, cache(0));
         withoutStateEventLogListener.expectOnlyCreatedEvent(1, cache(0));
         findServerAndKill(FirstServerAvailableBalancer.serverToKill);
         c.put(2, "two");
         withoutStateEventLogListener.expectFailoverEvent();
         withStateEventLogListener.expectFailoverEvent();
         withoutStateEventLogListener.expectNoEvents();
         withStateEventLogListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         c.remove(1);
         c.remove(2);
      } finally {
         killRemoteCacheManager(newClient);
      }
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

   public static class FirstServerAvailableBalancer implements FailoverRequestBalancingStrategy {
      static Log log = LogFactory.getLog(FirstServerAvailableBalancer.class);
      static InetSocketAddress serverToKill;
      private final RoundRobinBalancingStrategy delegate = new RoundRobinBalancingStrategy();

      @Override
      public void setServers(Collection<SocketAddress> servers) {
         log.info("Set servers: " + servers);
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

      @Override
      public SocketAddress nextServer() {
         return nextServer(null);
      }
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends EventLogListener<K> {}

}
