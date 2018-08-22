package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
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
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterExpirationEventsTest")
public class ClientClusterExpirationEventsTest extends MultiHotRodServersTest {

   static final int NUM_SERVERS = 2;

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
      injectTimeServices();
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

   private void injectTimeServices() {
      long now = System.currentTimeMillis();
      ts0 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(server(0).getCacheManager(), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(server(1).getCacheManager(), TimeService.class, ts1, true);
   }

   public void testSimpleExpired() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final EventLogListener<Integer> l = new EventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(key0, "one", 10, TimeUnit.MINUTES);
         l.expectOnlyCreatedEvent(key0);
         ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
         assertNull(remote.get(key0));
         l.expectOnlyExpiredEvent(key0);
      });
   }

   public void testFilteringInCluster() {
      // Generate key and add static filter in all servers
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      for (HotRodServer server : servers)
         server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory(key1));

      // We listen to all events, otherwise events that are filtered out could leak between tests
      final EventLogListener<Integer> allEvents = new EventLogListener<>(client(0).getCache());
      withClientListener(allEvents, r1 -> {
         final StaticFilteredEventLogListener<Integer> l = new StaticFilteredEventLogListener<>(r1);
         withClientListener(l, remote -> {
            allEvents.expectNoEvents();
            l.expectNoEvents();

            remote.put(key0, "one", 10, TimeUnit.MINUTES);
            allEvents.expectOnlyCreatedEvent(key0);
            l.expectNoEvents();

            remote.put(key1, "two", 10, TimeUnit.MINUTES);
            allEvents.expectOnlyCreatedEvent(key1);
            l.expectOnlyCreatedEvent(key1);

            // Now expire both
            ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
            ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

            assertNull(remote.get(key0));
            allEvents.expectOnlyExpiredEvent(key0);
            l.expectNoEvents();

            assertNull(remote.get(key1));
            allEvents.expectOnlyExpiredEvent(key1);
            l.expectOnlyExpiredEvent(key1);
         });
      });
   }

   public void testConversionInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final StaticCustomEventLogListener<Integer> l = new StaticCustomEventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(key0, "one", 10, TimeUnit.MINUTES);
         l.expectCreatedEvent(new CustomEvent(key0, "one", 0));
         remote.put(key1, "two", 10, TimeUnit.MINUTES);
         l.expectCreatedEvent(new CustomEvent(key1, "two", 0));

         // Now expire both
         ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
         ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

         assertNull(remote.get(key0));
         l.expectExpiredEvent(new CustomEvent(key0, "one", 0));
         assertNull(remote.get(key1));
         l.expectExpiredEvent(new CustomEvent(key1, "two", 0));
      });
   }

   public void testFilterCustomEventsInCluster() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final Integer key1 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
      final FilterCustomEventLogListener<Integer> l = new FilterCustomEventLogListener<>(client(0).getCache());
      withClientListener(l, new Object[]{key0}, null, remote -> {
         remote.put(key0, "one", 10, TimeUnit.MINUTES);
         l.expectCreatedEvent(new CustomEvent(key0, null, 1));
         remote.put(key1, "two", 10, TimeUnit.MINUTES);
         l.expectCreatedEvent(new CustomEvent(key1, "two", 1));

         // Now expire both
         ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
         ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

         assertNull(remote.get(key0));
         l.expectExpiredEvent(new CustomEvent(key0, null, 2));
         assertNull(remote.get(key1));
         l.expectExpiredEvent(new CustomEvent(key1, "two", 2));
      });
   }

   public void testNullValueMetadataExpiration() {
      final Integer key = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final EventLogListener<Integer> l = new EventLogListener<>(client(0).getCache());
      withClientListener(l, remote -> {
         Cache<Integer, String> cache0 = cache(0);
         CacheNotifier notifier = cache0.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class);
         byte[] keyBytes = HotRodClientTestingUtil.toBytes(key);
         // Note we are manually forcing an expiration event with a null value and metadata
         notifier.notifyCacheEntryExpired(keyBytes, null, null, null);
         l.expectOnlyExpiredEvent(key);
      });
   }
}
