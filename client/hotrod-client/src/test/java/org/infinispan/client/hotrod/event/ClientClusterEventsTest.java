package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterCustomEventLogListener;
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

   static final int NUM_SERVERS = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventFilterConverterFactory("filter-converter-factory", new FilterConverterFactory());
      servers.add(server);
      return server;
   }

   public void testEventForwarding() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = client(0).getCache();
            eventListener.expectNoEvents();
            remote.put(key0, "one");
            eventListener.expectOnlyCreatedEvent(key0, cache(0));
            remote.put(key1, "two");
            eventListener.expectOnlyCreatedEvent(key1, cache(0));
            remote.replace(key0, "new-one");
            eventListener.expectOnlyModifiedEvent(key0, cache(0));
            remote.replace(key1, "new-two");
            eventListener.expectOnlyModifiedEvent(key1, cache(0));
            remote.remove(key0);
            eventListener.expectOnlyRemovedEvent(key0, cache(0));
            remote.remove(key1);
            eventListener.expectOnlyRemovedEvent(key1, cache(0));
         }
      });
   }

   public void testFilteringInCluster() {
      // Generate key and add static filter in all servers
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      for (HotRodServer server : servers)
         server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory(key1));

      final StaticFilteredEventLogListener<Integer> eventListener = new StaticFilteredEventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = client(0).getCache();
            eventListener.expectNoEvents();
            remote.put(key0, "one");
            eventListener.expectNoEvents();
            remote.put(key1, "two");
            eventListener.expectOnlyCreatedEvent(key1, cache(0));
            remote.remove(key0);
            eventListener.expectNoEvents();
            remote.remove(key1);
            eventListener.expectOnlyRemovedEvent(key1, cache(0));
         }
      });
   }

   public void testConversionInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(0).getCache();
            eventListener.expectNoEvents();
            c3.put(key0, "one");
            eventListener.expectCreatedEvent(new CustomEvent(key0, "one", 0));
            c3.put(key1, "two");
            eventListener.expectCreatedEvent(new CustomEvent(key1, "two", 0));
            c3.remove(key0);
            eventListener.expectRemovedEvent(new CustomEvent(key0, null, 0));
            c3.remove(key1);
            eventListener.expectRemovedEvent(new CustomEvent(key1, null, 0));
         }
      });
   }

   public void testFilterCustomEventsInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final FilterCustomEventLogListener eventListener = new FilterCustomEventLogListener();
      withClientListener(eventListener, new Object[]{key0}, null, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = client(0).getCache();
            remote.put(key0, "one");
            eventListener.expectCreatedEvent(new CustomEvent(key0, null, 1));
            remote.put(key0, "newone");
            eventListener.expectModifiedEvent(new CustomEvent(key0, null, 2));
            remote.put(key1, "two");
            eventListener.expectCreatedEvent(new CustomEvent(key1, "two", 1));
            remote.put(key1, "dos");
            eventListener.expectModifiedEvent(new CustomEvent(key1, "dos", 2));
            remote.remove(key0);
            eventListener.expectRemovedEvent(new CustomEvent(key0, null, 3));
            remote.remove(key1);
            eventListener.expectRemovedEvent(new CustomEvent(key1, null, 3));
         }
      });
   }

}
