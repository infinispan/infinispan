package org.infinispan.client.hotrod.event;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.FilterCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;

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
      ts0 = new ControlledTimeService(0);
      TestingUtil.replaceComponent(server(0).getCacheManager(), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(0);
      TestingUtil.replaceComponent(server(1).getCacheManager(), TimeService.class, ts1, true);
   }

   public void testSimpleExpired() {
      final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> remote = client(0).getCache();
            eventListener.expectNoEvents();
            remote.put(key0, "one", 10, TimeUnit.MINUTES);
            eventListener.expectOnlyCreatedEvent(key0, cache(0));
            ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
            assertNull(remote.get(key0));
            eventListener.expectOnlyExpiredEvent(key0, cache(0));
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
            remote.put(key0, "one", 10, TimeUnit.MINUTES);
            eventListener.expectNoEvents();
            remote.put(key1, "two", 10, TimeUnit.MINUTES);
            eventListener.expectOnlyCreatedEvent(key1, cache(1));

            // Now expire both
            ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
            ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

            assertNull(remote.get(key0));
            eventListener.expectNoEvents();
            assertNull(remote.get(key1));
            eventListener.expectOnlyExpiredEvent(key1, cache(1));
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
            c3.put(key0, "one", 10, TimeUnit.MINUTES);
            eventListener.expectCreatedEvent(new CustomEvent(key0, "one", 0));
            c3.put(key1, "two", 10, TimeUnit.MINUTES);
            eventListener.expectCreatedEvent(new CustomEvent(key1, "two", 0));

            // Now expire both
            ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
            ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

            assertNull(c3.get(key0));
            eventListener.expectExpiredEvent(new CustomEvent(key0, "one", 0));
            assertNull(c3.get(key1));
            eventListener.expectExpiredEvent(new CustomEvent(key1, "two", 0));
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
            remote.put(key0, "one", 10, TimeUnit.MINUTES);
            eventListener.expectCreatedEvent(new CustomEvent(key0, null, 1));
            remote.put(key1, "two", 10, TimeUnit.MINUTES);
            eventListener.expectCreatedEvent(new CustomEvent(key1, "two", 1));

            // Now expire both
            ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
            ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

            assertNull(remote.get(key0));
            eventListener.expectExpiredEvent(new CustomEvent(key0, null, 2));
            assertNull(remote.get(key1));
            eventListener.expectExpiredEvent(new CustomEvent(key1, "two", 2));
         }
      });
   }

   public void testNullValueMetadataExpiration() {
      final Integer key = HotRodClientTestingUtil.getIntKeyForServer(server(0));
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            Cache<Integer, String> cache0 = cache(0);
            CacheNotifier notifier = cache0.getAdvancedCache().getComponentRegistry().getComponent(CacheNotifier.class);
            byte[] keyBytes = HotRodClientTestingUtil.toBytes(key);
            // Note we are manually forcing an expiration event with a null value and metadata
            notifier.notifyCacheEntryExpired(keyBytes, null, null, null);
            eventListener.expectOnlyExpiredEvent(key, cache(0));
         }
      });
   }
}
