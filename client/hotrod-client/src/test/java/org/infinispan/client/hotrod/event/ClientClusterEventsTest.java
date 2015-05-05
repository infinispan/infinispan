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
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.Random;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterEventsTest")
public class ClientClusterEventsTest extends MultiHotRodServersTest {

   static final int NUM_SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory());
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventFilterConverterFactory("filter-converter-factory", new FilterConverterFactory());
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
            eventListener.expectCreatedEvent(new CustomEvent(1, "one", 0));
            c3.put(2, "two");
            eventListener.expectCreatedEvent(new CustomEvent(2, "two", 0));
            c3.remove(1);
            eventListener.expectRemovedEvent(new CustomEvent(1, null, 0));
            c3.remove(2);
            eventListener.expectRemovedEvent(new CustomEvent(2, null, 0));
         }
      });
   }

   /**
    * TODO I suspect there is a another utility method somewhere that does exactly this.
    *
    * @param nodeIdx
    * @param exceptThese don't give me these
    * @return a scooby snack
    */
   private Integer newMagickKey(int nodeIdx, Integer... exceptThese) {
      Address nodeAddress = cache(nodeIdx).getAdvancedCache().getRpcManager().getAddress();
      ConsistentHash ch = cache(nodeIdx).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      Random r = new Random();
      Marshaller m = client(nodeIdx).getMarshaller();
      for (int i = 0 ; i < 10000; i++) {
         Integer key = r.nextInt();
         boolean badKey = false;
         if (exceptThese != null) {
            for (Integer e : exceptThese) {
               if (e.equals(key)) {
                  badKey = true;
                  break;
               }
            }
         }
         if (!badKey) {
            try {
               byte[] keyBytes = m.objectToByteBuffer(key, 100);
               if (nodeAddress.equals(ch.locatePrimaryOwner(keyBytes))) {
                  return key;
               }
            } catch (Exception e) {
               break;
            }
         }
      }
      throw new RuntimeException("I tried hard but failed to find a key local to node " + nodeIdx + " :(");
   }

   public void testFilterCustomEventsInCluster() {
      // test with two keys primary-owned by same node and a third key primary-owned by a different node
      final Integer key1 = newMagickKey(1);
      final Integer key2 = newMagickKey(1, key1);
      final Integer key3 = newMagickKey(2, key1, key2);

      final FilterCustomEventLogListener eventListener = new FilterCustomEventLogListener();
      withClientListener(eventListener, new Object[]{key1}, null, new RemoteCacheManagerCallable(client(0)) {
         @Override
         public void call() {
            RemoteCache<Integer, String> c3 = client(2).getCache();
            c3.put(key1, "one");
            eventListener.expectCreatedEvent(new CustomEvent(key1, null, 1));
            c3.put(key1, "newone");
            eventListener.expectModifiedEvent(new CustomEvent(key1, null, 2));
            c3.put(key2, "two");
            eventListener.expectCreatedEvent(new CustomEvent(key2, "two", 3));
            c3.put(key2, "dos");
            eventListener.expectModifiedEvent(new CustomEvent(key2, "dos", 4));
            c3.put(key3, "three");
            eventListener.expectCreatedEvent(new CustomEvent(key3, "three", 1));
            c3.put(key3, "tres");
            eventListener.expectModifiedEvent(new CustomEvent(key3, "tres", 2));
            c3.remove(key1);
            eventListener.expectRemovedEvent(new CustomEvent(key1, null, 5));
            c3.remove(key2);
            eventListener.expectRemovedEvent(new CustomEvent(key2, null, 6));
            c3.remove(key3);
            eventListener.expectRemovedEvent(new CustomEvent(key3, null, 3));
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
         c.put(3, "three");
         statefulListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 3);
         c.remove(1);
         c.remove(2);
         c.remove(3);
      } finally {
         killRemoteCacheManager(newClient);
      }
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends FailoverEventLogListener<K> {}

}
