package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

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
      final EventLogListener<Integer> l = new EventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectOnlyCreatedEvent(key0);
         remote.put(key1, "two");
         l.expectOnlyCreatedEvent(key1);
         remote.replace(key0, "new-one");
         l.expectOnlyModifiedEvent(key0);
         remote.replace(key1, "new-two");
         l.expectOnlyModifiedEvent(key1);
         remote.remove(key0);
         l.expectOnlyRemovedEvent(key0);
         remote.remove(key1);
         l.expectOnlyRemovedEvent(key1);
      });
   }

   public void testFilteringInCluster() {
      // Generate key and add static filter in all servers
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      for (HotRodServer server : servers)
         server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory(key1));

      final StaticFilteredEventLogListener<Integer> l = new StaticFilteredEventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectNoEvents();
         remote.put(key1, "two");
         l.expectOnlyCreatedEvent(key1);
         remote.remove(key0);
         l.expectNoEvents();
         remote.remove(key1);
         l.expectOnlyRemovedEvent(key1);
      });
   }

   public void testConversionInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final StaticCustomEventLogListener<Integer> l = new StaticCustomEventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(key0, "one");
         l.expectCreatedEvent(new CustomEvent(key0, "one", 0));
         remote.put(key1, "two");
         l.expectCreatedEvent(new CustomEvent(key1, "two", 0));
         remote.remove(key0);
         l.expectRemovedEvent(new CustomEvent(key0, null, 0));
         remote.remove(key1);
         l.expectRemovedEvent(new CustomEvent(key1, null, 0));
      });
   }

   public void testFilterCustomEventsInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final FilterCustomEventLogListener<Integer> l = new FilterCustomEventLogListener<>(client(0).getCache());
      withClientListener(l, new Object[]{key0}, null, remote -> {
         remote.put(key0, "one");
         l.expectCreatedEvent(new CustomEvent(key0, null, 1));
         remote.put(key0, "newone");
         l.expectModifiedEvent(new CustomEvent(key0, null, 2));
         remote.put(key1, "two");
         l.expectCreatedEvent(new CustomEvent(key1, "two", 1));
         remote.put(key1, "dos");
         l.expectModifiedEvent(new CustomEvent(key1, "dos", 2));
         remote.remove(key0);
         l.expectRemovedEvent(new CustomEvent(key0, null, 3));
         remote.remove(key1);
         l.expectRemovedEvent(new CustomEvent(key1, null, 3));
      });
   }

}
